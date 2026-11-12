# Plan: #957 snapshot frame writer after Close

Objective: cover `ErrSnapshotFrameWriterClosed` and the idempotent `Close` of
`snapshotFrameWriter` (pkg/internal/engine/snapshot_frame_writer.go).

## Slices

1. Test file pkg/internal/engine/snapshot_frame_writer_closed_test.go:
   - (a) Write after Close returns (0, ErrSnapshotFrameWriterClosed); dst unchanged.
   - (b) a second Close returns nil; dst unchanged (no second terminal frame).
   - (c) for inputs of 0, 100, snapshotEncryptChunkSize and
     2*snapshotEncryptChunkSize+1 bytes, OpenSnapshotFrames yields exactly the
     bytes written before Close, also after a refused Write and second Close.
   - (d) a writer whose first emit failed returns the latched error from Write
     after Close, not ErrSnapshotFrameWriterClosed.
2. Per divergence from the docs: a failing test commit, then a fix commit in
   snapshot_frame_writer.go.

## Out of scope

Frame format, reader changes, any wire or on-disk version.
