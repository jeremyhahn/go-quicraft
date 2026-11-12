# #961: dragonboat RequestState concurrent ResultC and Release

## Objective

`RequestState.ResultC` (contrib/dragonboat/requeststate.go) builds the
NotifyCommit aggregate channel lazily under `rs.mu`; `Release` is documented
to reach the inner state once and to wait for every bridge goroutine. The
existing tests call both from one goroutine. Add race-detector coverage for
concurrent callers.

## Slices

1. `docs: plan for #961` (this file).
2. `test:` contrib/dragonboat/requeststate_concurrent_test.go, all
   functions named `TestRequestStateConcurrent_*`, states built from
   `quicraft.NewRequestStatePool().Get()`:
   - (a) 32 goroutines call `ResultC` on `WrapRequestState(inner, true)`:
     every caller gets the same channel, and `AppliedC` is still the apply
     channel (distinct from the aggregate).
   - (b) `WrapRequestStateWithCancel(inner, cancel, true)`: 8 goroutines call
     `Release` while 8 call `ResultC`; no panic, cancel runs exactly once
     (atomic counter), `runtime.NumGoroutine()` back within 5 of baseline.
   - (c) `ResultC` on a released state returns a non-nil channel and starts
     no goroutine.
   - (d) `notifyCommit` false: concurrent `ResultC` calls all return
     `AppliedC`.
3. Per divergence between code and docs: a failing `test:` commit, then a
   `fix:` commit in contrib/dragonboat/requeststate.go (or a doc fix there
   when the code is right).

Helpers take `context.Context` first, before `t *testing.T`.

## Acceptance

- `cd contrib/dragonboat && go test -race -count=3 -run RequestStateConcurrent .`
- `cd contrib/dragonboat && go vet .`
- `make lint` clean.

## Out of scope

Any other file; behaviour changes beyond a documented-contract bug; wire or
on-disk formats.
