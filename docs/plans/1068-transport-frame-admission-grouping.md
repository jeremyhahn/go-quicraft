# Plan: #1068 transport frame admission grouping

Test gap under #744 (v1.0.0 bar).

## Objective

Pin the contract of `messageFrameError`, `messageFrameable` and
`sendableGroup` in `pkg/internal/transport/frame.go` with direct unit tests.

## File map

- `docs/plans/1068-transport-frame-admission-grouping.md` (this file)
- `pkg/internal/transport/frame_admission_test.go` (new)
- `pkg/internal/transport/frame.go` only for a bug found

## Assertions

With `h := batchHeaderSize("127.0.0.1:5000")`:

1. `messageFrameError` is nil at exactly `proto.MaxEntriesPerMessage` entries
   and `proto.ErrTooManyMessages` one entry past it.
2. `messageFrameError` for a heartbeat is nil at size `MaxFrameSize-h` and
   `ErrPayloadTooLarge` at `MaxFrameSize-h+1`; `messageFrameable` agrees.
3. `sendableGroup` returns `(0, 0, ErrTooManyMessages)` when `msgs[0]` is
   unframeable.
4. Three heartbeats group whole; payload equals `MessageBatch.Size()`.
5. A group stops before an unframeable message (count 1, nil error).
6. A group stops at `proto.MaxMessagesPerBatch` messages, and at the frame
   size (two 9 MiB replicates go one per frame).

## Bug handling

A divergence of code from documentation gets a failing test commit, then a
fix commit in `frame.go`. A wrong doc comment is corrected in the same file.
