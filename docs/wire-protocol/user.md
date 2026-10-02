# Wire Protocol — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/proto` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

Most people embedding go-quicraft never import `pkg/proto`. You reach it in
three situations: your state machine keeps the commands it is given, you are
reading a log or a snapshot by hand to diagnose something, or you are changing
a layout. The third is where the damage happens.

## Before you start

- Go 1.27.0 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- Every node and every consuming repository built from the same commit. Until
  the first production release, format versions are pinned and layouts change
  in place, so two binaries from different pre-release commits may be mutually
  unreadable while both stamp the same version. The failure is a decode error, not a version
  mismatch that names itself.

## Task: keep a command past `Update()`

Your state machine receives `sm.Entry` values: an `Index` and a `Cmd`, already
decompressed, with no entry type to inspect. Whether `Cmd` is yours to keep
depends on `Config.ZeroCopyEntryCmd`:

- **false (the default)**: `Cmd` is an owned copy. Keep it.
- **true**: `Cmd` aliases an internal buffer that is reused after `Update()`
  returns. Copy anything you keep, with `CopyCmd`.

```go
func (s *MyStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for _, e := range entries {
		// WRONG under ZeroCopyEntryCmd: s.pending would point into a reused buffer.
		// s.pending = append(s.pending, e.Cmd)

		s.pending = append(s.pending, e.CopyCmd())
	}
	return nil
}
```

If you are not certain which mode you run under, copy. The cost is one
allocation per retained command; the alternative is a bug that reads another
entry's data.

## Task: read `proto.Entry` directly

If you decode log entries yourself, mask the type before comparing it:

```go
switch e.Type & proto.EntryTypeMask { // always mask
case proto.EntryNormal:
	// ordinary command
case proto.EntryConfigChange:
	// membership change
}

if e.Type&proto.EntryCompressFlag != 0 {
	// Cmd is compressed. The algorithm is the writer's configuration
	// (config.Config.EntryCompressionType); it is not carried on the entry.
}
```

Comparing `e.Type` without masking fails as soon as compression is enabled,
because the flag is bit 4 of the same field.

After `UnmarshalFrom`, `proto.Entry.Cmd`, `SnapshotChunk.Data` and
`SnapshotFile.Metadata` alias the buffer they were decoded from. Copy them
before you reuse or pool that buffer.

## Task: bound what you produce

If you build `MessageBatch`es, `Message`s or membership maps yourself (you
should not need to, but the types are exported), respect these bounds:

| Bound | Value |
|---|---|
| messages per batch | 10,000 |
| entries per message | 10,000 |
| membership entries per map | 10,000 |
| batch bytes | 64 MB |
| `Entry.Cmd` | 8 MB |
| address / node identity | 256 bytes |
| filepath | 4096 bytes |

Exceeding one does not produce a friendly error at the peer. The peer rejects
the frame, and at the sender a rejected frame looks like a broken connection.
`MarshalTo` enforces the same bounds, so you find out at the sender first.

## Task: size the receive budget

A decoded frame is larger than its bytes: a 100-byte heartbeat becomes a
320-byte `Message` allocation, and a membership map entry costs 12 bytes on
the wire and roughly 57 in a Go map. `MaxDecodeMemoryFactor` (8) is the
measured ceiling on that ratio; the measured worst case is 5.1x.

The transport charges every inbound frame at eight times its wire length
against `TransportConfig.MaxMessageReceiveMemory`, so that setting is a heap
budget. The default 256 MB admits at most 32 MB of frames in flight; raise it
if many peers send large batches at once.

## Task: inspect a log or a snapshot by hand

There is no dump tool, deliberately; see [`cli.md`](cli.md). Use the offset
tables in [`engineering.md`](engineering.md) with `xxd`. Two orientation
points:

- **A `Membership` section starts with a version byte**, currently `1`. That
  byte is your anchor when you are lost in a record.
- **A `MessageBatch` starts with `BinVer`**, deliberately first, so a receiver
  can reject a foreign wire version before parsing anything else. If the first
  eight bytes are not `01 00 00 00 00 00 00 00`, stop: you are at the wrong
  offset or looking at a frame from a different build.

All `pkg/proto` integers are little-endian.

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
   field or a map. A decoder that clamps its map size hint has exceeded the
   bound before, at 9.8x.
6. **If the type is reachable from `MessageBatch` or `SnapshotChunk`**, which
   includes the embedded persisted ones, that is a `WireVersion` bump from the
   first production release onward. The transport version is a supremum.

Adding a field to a fixed header moves every offset after it. Insert eight
bytes before `Entry.CmdLen` and an older reader takes the new field's low four
bytes as the command length. That is the failure you are risking.

## Troubleshooting

**`ErrUnsupportedVersion`.** The sender and receiver were built from different
commits, or you are pointing a reader at a data directory another build wrote.
Rebuild everything from one commit. Do not relax the check.

**`ErrBufferTooSmall` on marshal.** Allocate `Size()` bytes, not a guess.

**`ErrInvalidEntryType`.** Either a genuinely unknown type, or reserved bits
above bit 4 are set. They are rejected rather than masked, on purpose.

**`ErrEntrySizeLimitExceeded`.** A proposal above 8 MB. The effective cap at
`Propose()` is `min(MaxProposalPayloadSize, MaxEntrySize)`. Split the payload
or put it behind a reference; do not raise the constant.

**`ErrDuplicateMapKey`.** A membership map repeated a key. Encoding writes each
key once in ascending order, so a repeat means the bytes were not produced by
this encoder.

**`ErrSnapshotWithoutPosition`.** You are marshalling a `Message` whose
embedded `Snapshot` has content but zero `Index` and `Term`.

**A state machine reads another entry's data, or garbage that changes between
runs.** You kept an `sm.Entry.Cmd` past `Update()` with `ZeroCopyEntryCmd`
on. See the copy rule above.

**Fewer frames are admitted than the receive budget suggests.** Each frame is
charged at `MaxDecodeMemoryFactor` times its wire length. Raise
`MaxMessageReceiveMemory`.
