# Plan: #1067 dragonboat typed error contracts

Test gap (v1.0.0 bar, tracked under #744).

## Objective

Pin the documented behaviour of the typed errors in
`contrib/dragonboat/errors.go`: `AdapterError`, `ShardSettingsError`,
`RemovalMarkerError` and `CompactionIndexNotSupportedError`, and what
`translateError` and `IsTempError` do with them.

## File map

- `docs/plans/1067-dragonboat-typed-error-contracts.md` (this file)
- `contrib/dragonboat/errors_typed_test.go` (new)
- `contrib/dragonboat/errors.go` only if a divergence from the docs is found

## Assertions

1. `AdapterError` message is `dragonboat: untranslated quicraft error: <cause>`
   and `errors.Unwrap` returns the cause.
2. `ShardSettingsError` message is `dragonboat: invalid shard settings: <cause>`;
   it matches `ErrInvalidShardSettings` through a wrap and does not match
   `ErrInvalidOption` or `ErrInvalidTarget`.
3. `RemovalMarkerError` message is
   `dragonboat: removed-replica marker <op> failed: <path>: <cause>` and it
   unwraps to its cause.
4. `translateError` returns a `ShardSettingsError` pointer unchanged and a
   `CompactionIndexNotSupportedError` unwrapped (not an `AdapterError`).
5. `translateError` wraps a `RemovalMarkerError` in an `AdapterError` from
   which the marker error and its cause stay reachable.
6. `IsTempError` is false for all three typed errors.

Every test name contains `ErrorContract`.

## Acceptance

- `cd contrib/dragonboat && go test -race -count=1 -run ErrorContract ./`
- `cd contrib/dragonboat && go vet ./`
- `make lint`
