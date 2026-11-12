# Plan: #1010 waldb segment.CanFit gives the same verdict Write applies

Test gap under the v1.0.0 bar (#744). `segment.CanFit`
(pkg/logdb/waldb/segment.go) documents four verdicts — nil, `ErrSegmentFull`,
`ErrRecordTooLarge`, `ErrClosed` — and says `Write` applies exactly the same
predicate, so a nil from CanFit guarantees the next Write is not refused.

## Tests (pkg/logdb/waldb/segment_canfit_test.go)

Every test name contains `SegmentCanFitMatchesWrite`; every helper takes
`context.Context` first.

- (a) After a prior record that crossed a block boundary (bytes on disk and in
  the block buffer): search the largest L for which CanFit is nil; Write(L)
  succeeds and the following Sync keeps the segment within maxFileSize; on a
  fresh segment in the same state CanFit(L+1) is `ErrSegmentFull` and Write
  of L+1 bytes is refused with the same error, leaving the segment unchanged.
- (b) The same with a partial write that leaves bytes only in the block buffer.
- (c) On an empty segment, the largest fitting L plus one, and a length larger
  than the segment, return `ErrRecordTooLarge` from both CanFit and Write.
- (d) After Close, CanFit returns `ErrClosed`.

## Suspected divergence

The `ErrRecordTooLarge` bound is computed with `bytesNeeded` at the segment's
CURRENT block offset. Starting mid-block splits the record into one more
chunk than from an empty segment, so a record that fits a fresh segment can
be reported as `ErrRecordTooLarge` (permanent: the caller does not rotate)
instead of `ErrSegmentFull`. The doc says `ErrRecordTooLarge` is only for a
record that could not fit in any segment. If the test confirms it: a failing
test commit, then a fix commit that bounds `ErrRecordTooLarge` by the framed
size from an empty segment. No on-disk format change.
