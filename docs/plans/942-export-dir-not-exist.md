# Plan: #942 ErrDirNotExist for a missing or non-directory ExportPath

## Objective

Close the v1.0.0 bar test gap: no test asserts `ErrDirNotExist`, whose only
source is `checkExportPath` in contrib/dragonboat/nodehost.go, called by
`SyncRequestSnapshot` and `RequestSnapshot` before the host is used.

## Slices

1. `docs: plan for #942` (this file).
2. `test:` contrib/dragonboat/snapshot_export_path_test.go:
   - `checkExportPath` accepts an empty `ExportPath` and an existing
     `t.TempDir()`;
   - it returns `ErrDirNotExist` for a missing path and for a regular file;
   - `SyncRequestSnapshot` (context with a deadline) and `RequestSnapshot`
     (valid timeout) return `ErrDirNotExist` (`errors.Is`) for both cases on
     a NodeHost with no running host (the check precedes any host use);
   - `translateError(ErrDirNotExist)` returns it unchanged.
3. Per divergence from the docs: a failing `test:` commit, then a `fix:`
   commit in contrib/dragonboat/nodehost.go.

## Out of scope

- Any change to snapshot export behaviour beyond a documented-behaviour fix.
- Wire or on-disk formats; files outside the brief's file map.
