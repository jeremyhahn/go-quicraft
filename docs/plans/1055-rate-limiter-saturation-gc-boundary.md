# Plan: #1055 rate limiter saturation and follower GC boundary

Test gap under the v1.0.0 bar (#744).

## Objective

Pin three documented behaviours of `pkg/internal/server/rate.go`:

- `RateLimiter.Increase` saturates at `math.MaxUint64` instead of wrapping,
  and the limiter stays tripped (`RateLimited()` true); the same through
  `InMemRateLimiter.Increase`/`Get`.
- `Decrease` to or below zero gives exactly zero, and `Decrease` on zero
  stays zero, for both limiters.
- `InMemRateLimiter.gc` keeps a follower state for exactly `gcTick` ticks
  and removes it on the next `Tick`.

## File map

- `docs/plans/1055-rate-limiter-saturation-gc-boundary.md` (this file)
- `pkg/internal/server/rate_saturation_test.go` (new, package `server`)
- `pkg/internal/server/rate.go` only if the code diverges from its docs

## Acceptance

- `go test -race -count=1 -run 'Saturat|GCBoundary' ./pkg/internal/server/`
- `go vet ./pkg/internal/server/`
- `make lint` clean
