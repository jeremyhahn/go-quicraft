# Plan: dragonboat state machine errors reach NodeHost callers (#1006)

v1.0.0 bar test gap, tracked under #744.

## Objective

Show that an error a user state machine returns from `Lookup` or `Update`
reaches the `contrib/dragonboat` NodeHost caller with the cause intact.

## Under test

- `contrib/dragonboat/statemachine/adapters.go`: `regularAdapter`,
  `concurrentAdapter` and `diskAdapter` `Update` wrap a user error in
  `AdapterUpdateError`; their `Lookup` wraps it in `AdapterLookupError`.
  Both types `Unwrap` to the cause (`statemachine/errors.go`).
- `contrib/dragonboat/nodehost.go`: `SyncPropose`, `SyncRead`,
  `GetNoOPSession`, `StaleRead`.

## Tests (contrib/dragonboat/nodehost_sm_errors_test.go)

Every test name contains `NodeHostSMError`; every helper takes
`context.Context` first, before `*testing.T`.

1. Regular state machine whose `Lookup` returns a sentinel: `SyncRead` and
   `StaleRead` return an error that `errors.Is` matches to the sentinel and
   `errors.As` resolves to `*statemachine.AdapterLookupError`.
2. The same for a concurrent and an on-disk state machine.
3. A state machine whose `Update` returns an error: `SyncPropose` returns an
   error and no result, within its deadline.
4. A later `SyncPropose` on that shard returns an error before its full
   deadline instead of hanging.

## Bugs

Where the code differs from its documentation: a failing test commit, then
a fix commit in `contrib/dragonboat/statemachine/adapters.go`. A wrong doc
comment over correct code is corrected in the same file.

## Acceptance

- `cd contrib/dragonboat && go test -race -count=1 -run NodeHostSMError .`
- `cd contrib/dragonboat && go vet .`
- `make lint`
