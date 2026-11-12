# Plan: #1066 SnapshotOption.Validate contract

Test gap under the v1.0.0 bar (#744). No behaviour change is planned; a
divergence between code and documentation is fixed in a separate
test-then-fix commit pair.

## Contract under test

- `pkg/snapshot_option.go` `(*SnapshotOption).Validate` refuses only a
  non-empty `ExportPath` that `filepath.IsAbs` rejects, with
  `&SnapshotOptionError{Field: "ExportPath", Reason: "must be an absolute path"}`.
  It does not touch the filesystem.
- `SnapshotOptionError.Error` formats
  `quicraft: invalid snapshot option <Field>: <Reason>`; `Is` matches any
  `*SnapshotOptionError` target, so `ErrInvalidSnapshotOption` matches every
  option error, wrapped or not.
- `DefaultSnapshotOption` equals `SnapshotOption{}` and is not an export.
- `pkg/host.go` `(*Host).RequestSnapshot` checks, in order: `ctx.Err()`
  (ErrCanceled), `h.closed` (ErrClosed), `opt.Validate()`, then the shard
  lookup (ErrShardNotFound).

## Test file

`pkg/snapshot_option_validate_test.go` (package quicraft); every test name
contains `OptionValidate`:

- (a) zero value, `DefaultSnapshotOption`, bare `Exported: true` validate nil;
  `DefaultSnapshotOption == SnapshotOption{}`; `IsExport()` false.
- (b) absolute, non-existent directory validates nil.
- (c) `snaps/out`, `./x`, `..` give the exact message and Field `ExportPath`.
- (d) `errors.Is` matching: wrapped error and a foreign-field option error
  match `ErrInvalidSnapshotOption`; the option error does not match
  `ErrShardNotFound`; a plain error does not match the sentinel.
- (e) host with no shard: relative path gives a nil RequestState and the
  option error (not ErrShardNotFound); absolute path gives ErrShardNotFound.
- (f) closed host gives ErrClosed; cancelled context gives ErrCanceled.

## Acceptance

`go test -race -count=1 -run OptionValidate ./pkg/`, `go vet ./pkg/`,
`make lint`; diff limited to the File map.
