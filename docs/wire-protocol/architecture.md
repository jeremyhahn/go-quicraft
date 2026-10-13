# Wire Protocol — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/proto` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## The problem

A Raft implementation serializes the same handful of types on two very different
paths: onto the network, where both ends are live processes and a mismatch costs
one rejected connection, and onto disk, where the bytes outlive the binary that
wrote them and are replayed forever. Those paths need one format and two version
namespaces, and the difference between them is easy to get wrong in a way that
strands a released cluster.

`pkg/proto` is that one format.

## The shape chosen: fixed-layout hand-written codecs

No protobuf, no varints, no type tags, no code generation, no dependency outside
the standard library.

- Every `uint64` is 8 bytes little-endian; every `uint32` is 4 bytes
  little-endian; every `bool` is a `uint64` 0 or 1.
- Fixed-header types have statically known field offsets: `Entry` 68 bytes,
  `Message` 88, `MessageBatch` 24.
- Variable-length fields carry a 4-byte length prefix.
- Maps are written with keys sorted ascending, so identical membership
  configurations produce identical bytes.
- `Entry.Cmd`, `SnapshotChunk.Data` and `SnapshotFile.Metadata` **alias** the
  source buffer after decode.
- Every bound is validated **before** allocation, on both the read and the write
  side, so no type in this package can emit bytes its own decoder refuses.

### Rejected: protobuf

Protobuf buys schema evolution and cross-language readers. Neither is a
requirement here: the only consumers are Go, and the evolution story is the
pinning policy below. What it costs is a code-generation step in the build, a
dependency in a library that keeps its dependencies near zero, and varint
decoding on the hottest path in the repository. A heartbeat is 100 bytes and
the step worker parses one per peer per tick.

### Rejected: reflection-based encoding

Same throughput objection, plus the offsets stop being statically knowable,
which is what makes the layout auditable: a reviewer can read the offset table
and check it against the struct.

### Rejected: booleans as one byte

They are widened to 8 so every field in a fixed-header struct stays 8-byte
aligned. The three bytes per bool are cheaper than the alignment branch.

### Rejected: reserved padding for future fields

There is none, deliberately. Reserved bytes are only useful if a reader that
does not understand them can skip them, and every decoder here gates on exact
sizes. `Entry.Type`'s upper 59 bits look like the exception and are not: they
are **rejected** rather than ignored, so they buy no forward compatibility at
all. Masking them would let a sender attach 59 bits of its own and have the
receiver agree to a type it read as something else, on an entry that is then
committed to the Raft log and replayed forever.

## Two version namespaces holding one value

This is the design decision most likely to be undone by accident, so it is
stated here rather than left in a code comment.

| Namespace | Constant | Stamped into | Lifetime of the bytes |
|---|---|---|---|
| **Transport-scoped** | `WireVersion` | `MessageBatch.BinVer`, `SnapshotChunk.BinVer`, mirrored by the frame header byte in the transport | both ends are live processes; a mismatch costs one rejected connection and nothing durable |
| **Persisted-scoped** | `membershipFormatVersion` | the leading byte of every `Membership` section, including the `Cmd` of the `EntryInitialMembership` entry at log index 1 (stored in the WAL) and the header of an exported snapshot artifact | outlives the process, and the binary, that wrote it |

Both are `1` today and **must not be derived from each other**. `Membership` is
the bootstrap configuration at log index 1, so if the persisted version were
derived from `WireVersion`, the first post-1.0 transport-only bump would
simultaneously change the version byte every replica expects at index 1 and
leave a released cluster unable to replay its own log.

### The transport version is a supremum

Independent does not mean unrelated. `Membership` is embedded in `Snapshot`,
which is embedded in `Message`, which is embedded in `MessageBatch`, and it is
embedded directly in `SnapshotChunk`. Both of those gate on `BinVer ==
WireVersion` by **exact** equality, so a `Membership` layout change alters the
transport bytes without altering the discriminator that is supposed to describe
them: two dialects stamping one version number.

**The rule, from the first production release onward: bump `WireVersion`
whenever ANY type reachable from `MessageBatch` or `SnapshotChunk` changes
layout, including the embedded persisted ones.** The transport number is the
supremum of every layout version on the wire, never a number that tracks only
the transport-only fields.

The converse does **not** hold: `WireVersion` may move without
`membershipFormatVersion` moving, because a transport-only change leaves every
persisted byte alone. That asymmetry is why the compile-time assertion in
`membership.go` is `>=` and not `==`. It holds trivially at `1 >= 1` and is
retained precisely because it is what stops the two drifting apart at the bump
that matters.

## Pre-release pinning, and why the version byte stays anyway

go-quicraft has not made a production release. Until it does, **every** wire and
persisted format version is pinned at its current value (1 for `WireVersion`,
`membershipFormatVersion`, the transport frame header and the WAL's membership
section version), and every decoder gates on exact equality.

A layout change during this period is made **in place, at version 1**, and the
consuming repositories are updated in the same change. Development clusters are
wiped and re-synced rather than migrated. There is no dual-read path, no
migration window, and no legacy decode case, because there is no deployed data
and no deployed peer whose bytes anyone has promised to keep reading.

The consequence is blunt and intended: **two binaries built from different
pre-release commits may be mutually unreadable while both stamp version 1.**
Mixing them, over the wire or over a data directory one of them wrote, is
unsupported, and the failure it produces is a decode error, not a version
mismatch that names itself. Rebuild every node and every consuming repository
from the same commit.

Pinning the *value* is not the same as removing the *mechanism*. Every format
still writes its version and still refuses anything that is not 1, for two
reasons:

1. **It is what makes the 1.0 bump possible at all.** A format with no
   discriminator cannot be superseded in band: a section with no version byte
   is byte-indistinguishable from one whose first field happens to start with
   the same value.
2. **It turns a commit skew into a clean refusal instead of a misparse.** The
   wrong bytes are rejected at the gate rather than interpreted at the wrong
   offsets.

From the first production release the numbers mean what they normally mean: a
bump implies a decoder taught the old layout, a migration story, and a supported
window that widens rather than moves.

## Data flow

```
   Proposal            Replication                Persistence
   ────────            ───────────                ───────────
 Entry.MarshalTo ──▶ Message ──▶ MessageBatch ──▶ QUIC frame
        │                                          (transport/)
        │
        └────────────────────────────────────────▶ WAL record
                                                   (logdb/waldb)
                                                        │
   Snapshot ──▶ SnapshotChunk ──▶ QUIC frame            │
   SnapshotFile                                    snapshot.dat
```

Nothing in this package touches a socket or a file. It converts between structs
and `[]byte`; the transport and the WAL own the bytes on either side. That is
what makes the **buffer ownership rules** in
[`engineering.md`](engineering.md#buffer-ownership-rules) the load-bearing part
of the contract rather than an optimization note: a decoded `Entry.Cmd` points
into a pooled receive buffer, and every consumer either copies it or finishes
with it before the buffer is returned.

## Trust boundary

**Every input to `UnmarshalFrom` is hostile.** These bytes arrive from the
network, and the peer is authenticated by mTLS but its payload is not
trustworthy: a compromised or buggy peer is exactly the case the bounds exist
for.

- Limits are checked **before** allocation, never after. A crafted count that
  would drive a proportional `make()` is refused first.
- Counts are cross-checked against the bytes actually remaining, so a short
  buffer claiming a large count cannot size a map.
- A length prefix above `math.MaxInt32` is rejected before any narrowing
  conversion, because `int(len)` is negative for those values on a 32-bit
  platform and would turn the bounds check into a slice panic.
- Duplicate map keys are rejected: `putMap` writes each key once in ascending
  order, so a repeat is a non-canonical encoding.
- **A wire-byte budget is not a memory budget.** `MaxDecodeMemoryFactor` (8)
  records how much heap one decode may claim as a multiple of the frame's wire
  length, and it is measured by a test rather than asserted. The transport
  charges each inbound frame at that multiple against
  `MaxMessageReceiveMemory`, so the budget bounds decoded heap rather than
  bytes read.

## Component boundaries

- **`pkg/proto`** owns every serializable type and its codec, and depends on
  nothing in this repository.
- **`pkg/internal/transport`** frames `MessageBatch` and `SnapshotChunk` and
  owns the frame header byte that mirrors `WireVersion`.
- **`pkg/logdb`** defines its **own** storage-layer types (`Entry`, `State`,
  `Membership`, `Snapshot`, `Bootstrap`, `Update`) rather than reusing these,
  deliberately, to keep serialization and persistence separable.
- **`pkg/logdb/waldb`** owns the WAL's own `membershipSectionVersion`, a third
  discriminator, for the membership section of its snapshot records. That
  section has its own layout, independent of `proto.Membership`, and is gated
  the same way.


## What this feature deliberately does not do

- **No schema evolution.** No optional fields, no unknown-field preservation, no
  wire-compatible field addition. A layout change is a coordinated change.
- **No cross-language support.** Go only.
- **No compression.** `EntryCompressFlag` records only *that* `Cmd` is
  compressed; the algorithm is a local configuration value that never reaches an
  `Entry`. It reaches the wire only as `Snapshot.Compression`, which describes a
  snapshot data file.
- **No encryption or authentication.** Confidentiality at rest is
  [`../seal-barrier/`](../seal-barrier/architecture.md); in transit it is mTLS
  on [`../quic-transport/`](../quic-transport/architecture.md).
- **No transport.** No sockets, no files, no framing beyond the type layouts.
- **No `Message.Origin` on the wire.** It is a process-local field describing
  where a message came from; putting it on the wire would let a sender assert
  its own provenance.
