# Plan: #1073 nonce tracker and invocation counter edge contracts

Test gap for the v1.0.0 bar (tracked under #744).

## Contracts under test (pkg/seal/aead_tracking.go)

- `SetMaxTrackedNonces` with a value <= 0 removes the ceiling.
- A zero-value `InMemoryNonceTracker` (no epoch) answers `Contains`, `Count`,
  `Saturated`, `IsEnabled`, `CheckAndRecordNonce` and `Clear` without panicking.
- `Clear` on a disabled tracker keeps the recorded history and only lifts
  saturation; re-enabling resumes reuse detection against that history.
- `GetUsagePercentage` stays in [0.0, 100.0] even after `Seed` sets a count
  above the limit, and `ShouldRotate` is true there.

## Work

1. New `pkg/seal/aead_tracking_edges_test.go`; every test name contains
   `TrackingEdge`.
2. Where code diverges from its documentation: a failing test commit, then a
   fix commit in `pkg/seal/aead_tracking.go`. Expected: `GetUsagePercentage`
   divides count by limit with no clamp, so a seeded count of twice the limit
   reports 200%.

## Acceptance

- `go test -race -count=1 -run TrackingEdge ./pkg/seal/`
- `go vet ./pkg/seal/`
- `make lint`
