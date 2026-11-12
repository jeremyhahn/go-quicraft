# Plan: #1025 Node snapshot-directory holds

Test gap under the v1.0.0 bar (#744). `pkg/internal/engine/node.go` keeps two
kinds of snapshot-directory hold under the snapshot-directory lock:

- pins (`PinSnapshotDir`, `UnpinSnapshotDir`, `IsSnapshotDirPinned`), counted
  per `filepath.Clean(dir)`, documented to nest;
- claims (`ClaimSnapshotDir`, `ReleaseSnapshotDir`, `IsSnapshotDirClaimed`),
  keyed by index.

`GCSparedSnapshotDirs` returns every pinned directory plus the cleaned
`SnapshotSaveDir(index)` of each claim. The existing test covers one pin and
one claim only.

## Test

New `pkg/internal/engine/node_snapshot_dir_holds_test.go`, every function
named `...SnapshotDirHolds...`, all calls under `LockSnapshotDir`:

- (a) a new node: empty spared list, nothing pinned or claimed;
- (b) two pins on one directory need two unpins; the directory is listed once;
- (c) pins on `dir/` and `dir/./` are seen and ended through `dir`;
- (d) unpinning a never-pinned directory changes nothing; a later single pin
  needs one unpin;
- (e) claims on 100 and 200 list both save directories; releasing 100 removes
  only it; releasing an unclaimed index is a no-op;
- (f) a pin and a claim together give the union.

A divergence between code and documentation gets a failing test commit, then
a fix commit in `node.go` (or a doc fix where the code is right).

## Acceptance

`go test -race -count=1 -run SnapshotDirHolds ./pkg/internal/engine/`,
`go vet ./pkg/internal/engine/`, `make lint`; only the File map files change.
