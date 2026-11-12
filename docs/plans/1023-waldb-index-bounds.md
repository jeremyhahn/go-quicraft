# Plan: #1023 waldb index.Bounds test gap

`index.Bounds` (pkg/logdb/waldb/index.go) returns the first entry, the last
entry and the count under one read lock. `reconcileCompactionFloors` and
`EntryRange` in db.go derive a range from it, and `EntryRange` reports
`ErrLogGapDetected` when last-first+1 != count. No test calls it.

## Tests (pkg/logdb/waldb/index_bounds_test.go)

- (a) an empty index returns `indexEntry{}`, `indexEntry{}`, 0.
- (b) after appending Index 1..10, Bounds equals `First`, `Last` and `Len`
  field for field.
- (c) an overwrite at Index 6 gives 1, 6, 6; `Compact(3)` gives 4, 6, 3;
  `Compact(6)` gives the empty result.
- (d) one goroutine appends contiguous indexes, one compacts below the tail,
  a third calls Bounds 10000 times: every non-empty result has
  first.Index <= last.Index and last.Index-first.Index+1 == count.

Every test name contains `IndexBounds`; helpers take context.Context first.

## Bugs

If the code differs from its documentation: a failing test commit, then a
fix commit in index.go. A wrong doc comment is corrected in index.go.

Refs #1023
