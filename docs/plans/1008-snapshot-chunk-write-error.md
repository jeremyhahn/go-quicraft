# Plan: #1008 a failed snapshot chunk write names the shard, replica and chunk

Refs #1008 (v1.0.0 bar test gap, tracked under #744).

## Gap

`SendSnapshot` and `SendSnapshotStreaming` in `pkg/internal/transport/quic.go`
wrap every chunk write failure as `*SnapshotChunkWriteTimeoutError`, while the
doc comments in `pkg/internal/transport/errors.go` describe it only as the
per-chunk write deadline being exceeded. No test checks the fields or the cause
of the error a send returns when the receiver goes away mid-transfer.

## Test

New file `pkg/internal/transport/snapshot_write_error_test.go`, every test name
containing `ChunkWriteErrorCause`, for both send paths:

1. The sender's `OnSnapshotSendStarted` (fired after the header is written,
   before the first chunk) stops the receiver. A bounded loop retries with a
   fresh receiver until a send fails; no fixed sleeps.
2. The error is found by `errors.As` as `*SnapshotChunkWriteTimeoutError`;
   `ShardID`/`ReplicaID` match the request, `ChunkID` is below the chunk
   count, `errors.Unwrap` returns the non-nil stream error, and that cause is
   not a timeout.
3. `OnSnapshotSendAborted` fires for the shard, and the sender's `Stop`
   returns within a bound.

Helpers take `context.Context` first, before `t *testing.T`.

## Doc correction

The cause in (2) is a closed connection, not a deadline: the doc comments on
`ErrSnapshotChunkWriteTimeout` and `SnapshotChunkWriteTimeoutError` in
`errors.go` are corrected to say any chunk write failure is wrapped, the
deadline being one cause. Error strings and wire/on-disk formats do not change.

## Acceptance

- `go test -race -count=1 -run ChunkWriteErrorCause ./pkg/internal/transport/`
- `go vet ./pkg/internal/transport/`
- `make lint`
- `git diff --name-only 1be3cc2d..HEAD` lists only the files above.
