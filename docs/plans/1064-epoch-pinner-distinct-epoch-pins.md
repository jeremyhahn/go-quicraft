# Plan: #1064 epochPinner unit tests

Refs #1064 (v1.0.0 bar test gap, tracked under #744).

## Objective

Unit-test `epochPinner` (pkg/internal/engine/epoch_pinner.go) directly. Today it
is covered only through the snapshot frame writer, which checks net pin counts
after a whole writer cycle.

## File map

- docs/plans/1064-epoch-pinner-distinct-epoch-pins.md (this plan)
- pkg/internal/engine/epoch_pinner_test.go (new)
- pkg/internal/engine/epoch_pinner.go (only for a bug found)

## Assertions

Fakes: `newTestBarrier(epoch)` wrapped in `newPinCountBarrier`.

a. Seals at epochs 5, 5, 6, 5 each return nil error and the sealing epoch;
   `epochs()` is `[5 6]`; each epoch holds exactly one pin.
b. `track(0)` on a fresh pinner records nothing and releases nothing.
c. A failed seal (sealed barrier) returns an error, nil ciphertext and epoch 0,
   and pins nothing.
d. `releaseAll` after (a) brings both epochs to zero pins, never below zero,
   empties `epochs()`; a second `releaseAll` releases nothing.
e. After `releaseAll`, a seal at epoch 5 is tracked again (seen set reset).

A divergence between code and doc is a bug: failing test commit, then fix
commit in epoch_pinner.go.

## Acceptance

`go test -race -count=1 -run EpochPinner ./pkg/internal/engine/`,
`go vet ./pkg/internal/engine/`, `make lint`.
