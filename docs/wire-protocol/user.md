# Wire Protocol — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/proto` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Most people embedding go-quicraft never import `pkg/proto`. You reach it in
three situations: your state machine inspects the entries it is given, you are
reading a log or a snapshot to diagnose something, or you are changing a layout.
The third one is where the damage happens.

## Before you start

- Go 1.27.0 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- **Every node and every consuming repository built from the same commit.**
  Until the first production release, formats are pinned at version 1 and
  changed *in place*, so two binaries from different pre-release commits may be
  mutually unreadable while both stamp 1. The failure is a decode error, not a
  version mismatch that names itself.

## Task: read the entries your state machine is given

`sm.Entry` values arrive with the command already extracted, so most state
machines need nothing from this package. When you do read `Entry.Type`:

```go
switch e.Type & proto.EntryTypeMask {   // ALWAYS mask
case proto.EntryNormal:
    // ordinary command
case proto.EntryConfigChange:
    // membership change
}

if e.Type&proto.EntryCompressFlag != 0 {
    // Cmd is compressed. Which algorithm is YOUR configuration
    // (config.CompressionType) — it is not carried on the entry.
}
```

Comparing `e.Type` without masking silently fails the moment compression is
enabled, because the flag is bit 4 of the same field.

## Task: keep a command past `Update()`

This is the rule that bites, and the compiler will not enforce it.

After `UnmarshalFrom`, `Entry.Cmd` **aliases the buffer it was decoded from**,
and that buffer is pooled and reused. The same is true of `SnapshotChunk.Data`
and `SnapshotFile.Metadata`.

```go
func (s *MyStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
    for i, e := range entries {
        // WRONG — s.pending now points into a buffer that will be reused
        s.pending = append(s.pending, e.Cmd)

        // RIGHT
        cmd := make([]byte, len(e.Cmd))
        copy(cmd, e.Cmd)
        s.pending = append(s.pending, cmd)
    }
    return nil
}
```

The host-level switch for this is `Config.ZeroCopyEntryCmd`. When it is true,
`Entry.Cmd` aliases the internal buffer and **you must copy to retain it past
`Update()`**. When it is false you get a copy already. If you are not certain
which you are running under, copy — the cost is one allocation per retained
command and the alternative is a heisenbug that reads another shard's data.

## Task: bound what you produce

If you generate `MessageBatch`es, `Message`s or membership maps yourself — which
you should not need to, but the types are exported — respect these:

| Bound | Value |
|---|---|
| messages per batch | 10,000 |
| entries per message | 10,000 |
| membership entries | 10,000 |
| batch bytes | 64 MB |
| `Entry.Cmd` | 8 MB |
| address / node identity | 256 bytes |
| filepath | 4096 bytes |

Exceeding one does not produce a friendly error at the peer. The peer rejects
the frame, and at the sender a rejected frame is indistinguishable from a broken
connection.

## Task: size the receive budget

`TransportConfig.MaxMessageReceiveMemory` bounds **wire bytes**, reserved before
the body is read. The decoded form is larger — a 100-byte heartbeat becomes a
320-byte `Message` in a slice, and a membership map entry costs 12 bytes on the
wire and roughly 57 in a Go map.

Multiply by `MaxDecodeMemoryFactor` (8) to get what the process may actually
hold. A 256 MB budget admits frames that can occupy up to 2 GB of heap. The
measured worst case today is 5.1x; the headroom covers the Go runtime's map
layout, not room for new allocations.

## Task: inspect a log or a snapshot by hand

There is no dump tool, deliberately — see [`cli.md`](cli.md). Use the offset
tables in [`engineering.md`](engineering.md) with `xxd`. Two orientation points:

- **A `Membership` section starts with a version byte**, currently `1`. That
  byte is your anchor when you are lost in a WAL record.
- **A `MessageBatch` starts with `BinVer`**, deliberately first, so a receiver
  can reject a foreign wire version before parsing anything else. If the first
  eight bytes are not `01 00 00 00 00 00 00 00`, stop — you are either at the
  wrong offset or looking at a frame from a different build.

## Task: change a layout

Read [`architecture.md`](architecture.md) first, then:

1. **Make the change in place, at version 1.** Do not add a version case, a
   dual-read path, or a legacy decoder. There is no deployed data.
2. **Update every consuming repository in the same change**, and wipe and
   re-sync development clusters rather than migrating them.
3. **Check both codecs.** Every bound is enforced on the write side too,
   because an entry is committed to the Raft log before any peer parses it.
4. **Re-run the round-trip fuzz targets.** They assert byte-stability, which is
   what catches a field being silently masked instead of rejected.
5. **Check `TestDecodeMemoryAmplification`** if you added a variable-length
   field or a map. The bound has been exceeded once already, at 9.8x, by a
   decoder that clamped its map size hint.
6. **If the type is reachable from `MessageBatch` or `SnapshotChunk`** — which
   includes the embedded persisted ones — that is a `WireVersion` bump from the
   first production release onward. The transport version is a supremum.

Adding a field to a fixed header moves every offset after it. `Entry`'s header
went from 60 to 68 bytes when `OwnerID` was added, and a 0.4.x reader given a
0.5.x entry reads `OwnerID`'s low four bytes as `CmdLen`. That is the shape of
the failure you are risking.

## Troubleshooting

**`ErrUnsupportedVersion`** — the sender and receiver were built from different
commits, or you are pointing a reader at a data directory another build wrote.
Rebuild everything from one commit. Do not relax the check.

**`ErrBufferTooSmall` on marshal** — allocate `Size()` bytes, not a guess.

**`ErrInvalidEntryType`** — either a genuinely unknown type, or reserved bits
above bit 4 are set. They are rejected rather than masked, on purpose.

**`ErrEntrySizeLimitExceeded`** — a proposal above 8 MB. The effective cap at
`Propose()` is `min(MaxProposalPayloadSize, MaxEntrySize)`. Split the payload or
put it behind a reference; do not raise the constant.

**`ErrDuplicateMapKey`** — a membership map repeated a key. Encoding writes each
key exactly once in ascending order, so a repeat means the bytes were not
produced by this encoder.

**`ErrSnapshotWithoutPosition`** — you are marshalling a `Message` whose
embedded `Snapshot` has content but zero `Index` and `Term`.

**A state machine reading another shard's data, or garbage that changes between
runs** — you kept an `Entry.Cmd` past `Update()`. See the copy rule above.

**Memory growing faster than the receive budget suggests** — that is
`MaxDecodeMemoryFactor`. The budget counts wire bytes.
