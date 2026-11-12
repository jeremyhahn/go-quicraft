# Plan: #1003 Barrier.UnsealedC under concurrent Seal, Unseal and Reset

Refs #1003 (v1.0.0 bar test gap, tracked under #744).

## Contract under test (pkg/seal/barrier.go, `UnsealedC` doc)

- The channel is already closed when the barrier is unsealed now.
- Otherwise it is closed by the next transition to unsealed; one channel per
  sealed period, so every waiter of that period wakes, not just one.
- `setUnsealed(true)` closes the period's channel once and drops it; `Seal`
  and `Reset` start a new sealed period (`setUnsealed(false)`).

## Test file

`pkg/seal/unsealed_wait_race_test.go` (package seal), run under `-race`.
Every test name contains `UnsealedCRace`; every helper takes
`context.Context` first, then `*testing.T`. Waits are bounded by a context
deadline, never by sleeps.

- (a) Sealed barrier, M goroutines take `UnsealedC` and wait; after one
  `Unseal` all of them wake within the deadline.
- (b) One goroutine cycles `Seal`/`Unseal` a bounded number of times while
  waiter goroutines repeatedly take `UnsealedC` and wait; each wake is within
  the deadline; no double-close panic; the barrier ends unsealed and every
  waiter returns.
- (c) A channel taken while unsealed is already closed (also checked
  concurrently with readers).
- (d) `Seal`, `Reset`, then waiters taken after the reset are woken by the
  next `Initialize`; the test ends unsealed with no waiter blocked.

## Bugs

If code diverges from the doc: a failing `test:` commit, then a `fix:`
commit in pkg/seal/barrier.go. A doc comment wrong where the code is right is
corrected in barrier.go.

## Acceptance

- `go test -race -count=1 -run UnsealedCRace ./pkg/seal/`
- `go vet ./pkg/seal/`
- `make lint`
- `git diff --name-only 1be3cc2d..HEAD` lists only the File map files.
