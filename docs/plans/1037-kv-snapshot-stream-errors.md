# Plan: kv snapshot stream errors (#1037)

Test gap on the v1.0.0 bar. No production change is expected.

## Objective

Pin three documented behaviours of `MemoryStore` and `ConcurrentStore`
(`pkg/sm/kv/memory.go`, `pkg/sm/kv/concurrent.go`) over both backends:

1. `SaveSnapshot` returns the writer's own error when the write of the key
   bytes, the value length or the value bytes fails (single pair "k" -> "v";
   offsets 8, 9 and 13 bytes into the stream).
2. `RecoverFromSnapshot` on a stream holding the pair count, a key length and
   the key, then ending, returns `io.EOF`; the same stream plus two bytes of
   the value length returns `io.ErrUnexpectedEOF`.
3. A store seeded with "live" -> "v" whose recovery from either stream fails
   still returns "v" for "live" and the same `GetHash` as before.

## File map

- `docs/plans/1037-kv-snapshot-stream-errors.md` (this file)
- `pkg/sm/kv/snapshot_stream_errors_test.go` (new)
- for a bug found: `pkg/sm/kv/memory.go` or `pkg/sm/kv/concurrent.go`

## Acceptance

`go test -race -count=1 -run SnapshotStream ./pkg/sm/kv/`, `go vet ./pkg/sm/kv/`
and `make lint` pass; the diff lists only File map files.
