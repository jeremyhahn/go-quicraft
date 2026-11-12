# Plan: #943 HasAESNI and SelectOptimalAlgorithm test gap

## Objective

`HasAESNI` (pkg/seal/algorithm.go) has no test, and `SelectOptimalAlgorithm`
depends on it. Add pkg/seal/has_aesni_test.go asserting the documented
behaviour.

## Slices

1. This plan.
2. Test commit, pkg/seal/has_aesni_test.go:
   - `HasAESNI()` equals `cpu.X86.HasAES` on amd64, `cpu.ARM64.HasAES` on
     arm64, and false on any other `runtime.GOARCH`.
   - Repeated calls return the same value.
   - With `FIPSEnabled()` false: `SelectOptimalAlgorithm(false)` is
     `AlgAES256GCM` when `HasAESNI()` is true, else
     `softwareFallbackAlgorithm()`; `SelectOptimalAlgorithm(true)` is
     `AlgAES256GCM` either way. Skipped when FIPS mode is on.
3. Per divergence between code and docs: failing test commit, then fix commit
   in pkg/seal/algorithm.go (or a doc correction where the code is right).

## Out of scope

- Any other pkg/seal behaviour, wire or on-disk formats, go.sum.
