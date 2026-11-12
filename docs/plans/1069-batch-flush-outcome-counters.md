# Plan: #1069 batch flush outcome counters

## Objective

Pin the values of the batch aggregator's flush metrics, which no test reads
today: `quicraft_batch_batches_total{shard,status}` (attempted plus success or
failed per flush), `quicraft_batch_batch_size` (one sample per proposed batch),
`quicraft_batch_flush_duration_seconds` (one sample per proposer call) and
`quicraft_batch_flush_reason_total{reason}` for the size, bytes and close
reasons.

## File map

- `docs/plans/1069-batch-flush-outcome-counters.md` (this file)
- `pkg/batch/flush_counters_test.go` (new, package batch)
- `pkg/batch/aggregator.go` only for a bug the tests find

## Cases

- (a) size flush, clean propose: attempted 1, success 1, no failed series,
  reason size 1, batch_size count 1 sum 2.
- (b) size flush, proposer error: definite `*FlushError`, attempted 1,
  failed 1, no success series, flush_duration count 1.
- (c) close flush: reason close 1, size/time/bytes 0.
- (d) bytes overflow then close: bytes 1, close 1, attempted 2, success 2,
  two proposer calls.
- (e) caller cancels before the flush: attempted 1, failed 1, batch_size
  count 0, proposer never called.

## Acceptance

`go test -race -count=1 -run FlushCounters ./pkg/batch/`, `go vet ./pkg/batch/`,
`make lint` clean.
