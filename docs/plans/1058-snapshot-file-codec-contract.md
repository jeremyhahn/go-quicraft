# Plan: #1058 SnapshotFile codec contract

Test gap in `pkg/proto/snapshotfile.go`. New file
`pkg/proto/snapshotfile_contract_test.go` (package proto) asserts:

- (a) Filepath of `MaxFilepathLen+1` bytes: `MarshalTo` with a buffer of
  `Size()` bytes and with a nil buffer both return `ErrFilepathTooLong`, n 0.
- (b) Metadata of `MaxMetadataLen+1` bytes with a nil buffer returns
  `ErrMetadataTooLong`, not `ErrBufferTooSmall`.
- (c) a buffer holding a valid encoded Filepath "f" followed by 8 bytes:
  `UnmarshalFrom` returns `ErrBufferTooSmall`.
- (d) after marshal and `UnmarshalFrom` with non-empty Metadata, writing the
  source buffer at the Metadata offset changes `got.Metadata`.

Test names contain `SnapshotFileContract`. A divergence from the docs is a
failing test commit then a fix commit in `pkg/proto/snapshotfile.go`.
