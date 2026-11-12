# Plan: #1054 registry Count on conditional operations

Test gap under the v1.0.0 bar (#744).

## Objective

Pin how `pkg/internal/registry.Registry` keeps its atomic count when the
conditional operations run:

- (a) `CompareAndRemove` on a fresh registry (any `want`, including `""`)
  returns false and `Count()` stays 0.
- (b) `Register` then a matching `CompareAndRemove` returns true, `Count()`
  drops to 0 and `Resolve` fails with an error matching `ErrNodeNotFound`; a
  second identical `CompareAndRemove` returns false and `Count()` stays 0.
- (c) `RegisterIfAbsent`, `Remove`, `RegisterIfAbsent` with a new address
  stores the new address, reports true and `Count()` is 1.
- (d) a wrapped `Resolve` error still matches `ErrNodeNotFound`; an unrelated
  error does not.

## File map

- `docs/plans/1054-registry-count-on-conditional-ops.md` (this file)
- `pkg/internal/registry/registry_count_test.go` (new)
- `pkg/internal/registry/registry.go` / `errors.go` only for a bug found

## Acceptance

`go test -race -count=1 -run CountAfter ./pkg/internal/registry/` passes,
`go vet ./pkg/internal/registry/` passes, `make lint` is clean.
