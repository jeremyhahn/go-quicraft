# Plan: #948 NewEpochBarrier and MustNewEpochBarrier tests

## Objective

`NewEpochBarrier` (pkg/seal/barrier.go) is documented as calling
`NewBarrier`, and `MustNewEpochBarrier` as calling `MustNewBarrier`. Only
`MustNewEpochBarrier` on a valid config is tested. Close the gap.

## Slices

1. Test commit: pkg/seal/new_epoch_barrier_test.go (package seal).
   - (a) `NewEpochBarrier(DefaultBarrierConfig(7))` returns a non-nil
     barrier and nil error, `config.DeploymentID` is 7, and `IsSealed()`
     equals that of a `NewBarrier` barrier on the same config.
   - (b) A config with an unregistered algorithm (`unknownAlg`) makes both
     `NewBarrier` and `NewEpochBarrier` return a nil barrier and an error
     satisfying `errors.Is(err, ErrUnsupportedAlgorithm)`.
   - (c) `MustNewEpochBarrier` on that config panics with a string value
     that starts with `seal.MustNewBarrier: ` and contains the error text.
2. Per divergence between code and docs: a failing test commit, then a fix
   commit in pkg/seal/barrier.go. Reading shows none expected.

## Out of scope

- FIPS-mode refusals (`ErrFIPSUnavailable`, `ErrAlgorithmNotFIPSApproved`).
- Any change to barrier behaviour, wire or on-disk formats.
