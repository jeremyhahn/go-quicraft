# #958: dragonboat on-disk adapter passes ErrOpenStopped through Open

## Objective

`diskAdapter.Open` (contrib/dragonboat/statemachine/adapters.go) returns
`ErrOpenStopped` (errors.go) unwrapped, including when the inner state
machine wraps it with `%w`, and wraps every other error in
`*AdapterOpenError`. No test names it; add one.

## Slices

1. Test: contrib/dragonboat/statemachine/open_stopped_test.go
   - (a) inner `Open` returns `ErrOpenStopped`: `errors.Is` matches,
     `errors.As` to `*AdapterOpenError` does not, index 0.
   - (b) inner returns `fmt.Errorf("open: %w", ErrOpenStopped)`: passed
     through unchanged (same error value), same checks as (a).
   - (c) any other error: wrapped in `*AdapterOpenError` whose `Cause` is
     that error.
   - (d) the stopper channel given to the adapter reaches the inner `Open`.
   - (e) an adapter built by `WrapOnDiskCreateFunc` gives (a)-(c).
2. Per divergence between code and docs: failing test commit, then a fix
   commit in adapters.go (or a doc fix where the code is right).

## Out of scope

Other adapter methods, the concurrent and regular adapters, any wire or
on-disk format.
