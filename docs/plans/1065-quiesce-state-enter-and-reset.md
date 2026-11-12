# Plan: #1065 quiesceState enter and reset

Test gap under the v1.0.0 bar (#744). `pkg/internal/raft/quiesce.go` has no
test that drives a bare `quiesceState` through `newQuiesceState`, `tryEnter`,
`exit` and `recordActivity`.

## File map

- `docs/plans/1065-quiesce-state-enter-and-reset.md` (this plan)
- `pkg/internal/raft/quiesce_state_test.go` (new)
- `pkg/internal/raft/quiesce.go` only if the code diverges from its docs

## Assertions

a. `newQuiesceState(true, 4)`: enabled, threshold 40, livenessInterval 40,
   active/tick/livenessTick zero.
b. `newQuiesceState(false, 4)`: 1000 `tryEnter` calls return false; tick 0,
   not active.
c. Enabled, livenessTick preset to 7: calls 1..39 return false with tick == i;
   call 40 returns true, active, livenessTick 0; call 41 returns false and
   leaves tick 40.
d. Not active, tick 25: `recordActivity` gives tick 0, not active; the next 39
   `tryEnter` calls return false.
e. Active, livenessTick 9: `recordActivity` gives not active, tick 0,
   livenessTick 0; `exit` on a fresh active state gives the same.

Every test name contains `QuiesceState`; helpers take `context.Context` first.

## Acceptance

`go test -race -count=1 -run QuiesceState ./pkg/internal/raft/`, `go vet
./pkg/internal/raft/`, `make lint` clean, diff limited to the File map.
