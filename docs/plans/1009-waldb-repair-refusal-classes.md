# Plan: waldb Repair clears every refusal class it documents (#1009)

`waldb.Repair` (pkg/logdb/waldb/repair.go) is documented as the exit from
`ErrMidSegmentCorruption`, `ErrNonFinalSegmentCorruption`,
`ErrAcknowledgedDataLost` and `ErrRecordSequenceRegression`. repair_test.go
covers only the first. This change adds pkg/logdb/waldb/repair_classes_test.go,
which for each of the other three classes:

1. builds the fixture an existing recovery test already uses for that class
   (non-final segment corruption, damage below the acknowledged end, a
   durability-sequence regression above a lost page);
2. asserts the failed `Open` matches the class sentinel with `errors.Is` and
   carries a `*SegmentCorruptionError` naming the expected shard index and
   segment;
3. asserts `Repair` with a `TruncateAt` of `CleanEnd+1` is refused with
   `ErrRepairOffsetMismatch` and leaves every file in the directory
   byte-for-byte unchanged;
4. asserts `Repair` at `CleanEnd` succeeds, the next `Open` succeeds, and the
   entries written below the cut read back intact.

If Repair cannot clear a class its documentation names, that is a bug: a
failing test commit, then a fix commit in repair.go. A doc comment that is
wrong where the code is right is corrected in repair.go instead.

Test names contain `RepairEveryRefusalClass`; acceptance is
`go test -race -count=1 -run RepairEveryRefusalClass ./pkg/logdb/waldb/`.

Refs #1009
