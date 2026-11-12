# Plan: #1057 SaveSnapshotGated afterCapture gating

`StateMachine.SaveSnapshotGated` (pkg/internal/rsm/statemachine.go) documents
that afterCapture runs only once the capture completes, never on a save that
fails before it, and that the capture guards are released on every path.

New test file pkg/internal/rsm/after_capture_test.go, with a counting
afterCapture, asserts:

- closed StateMachine: `ErrClosed`, count 0; closed stopper: `ErrStopped`,
  count 0;
- session write fails (`limitedWriter`): error, count 0, and a following
  `Apply` of one entry returns within a bounded context (guards released);
- prepare error (concurrent, on-disk) and sync error (on-disk): the fake's
  error type, count 0;
- regular-tier save error: count 0 (the capture is the save itself);
  concurrent and on-disk save errors: `*testAdapterSaveError`, count 1
  (the capture completed in PrepareSnapshot before the stream failed).

A divergence from the documentation is a failing test commit then a fix
commit in statemachine.go or adapter.go.

Refs #1057
