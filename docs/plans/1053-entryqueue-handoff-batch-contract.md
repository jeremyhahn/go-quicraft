# Plan: #1053 EntryQueue hand-off and batch contract

Test gap under the v1.0.0 bar (tracked under #744).

## Objective

Pin the documented contract of `pkg/internal/queue/entryqueue.go`:

- `HandOff` never moves `handedOff` backwards, and handing off a later batch
  covers every earlier batch (`Taken` reports it taken).
- `AddTracked` refused for capacity leaves `*at` untouched; a zero `*at`
  is never taken.
- `SwapBatch` clears the recycled buffer before reusing it, so the buffer
  it later hands back holds no stale references past its length.
- `SwapBatch` on a closed queue returns length 0 and id 0.

## File map

- `docs/plans/1053-entryqueue-handoff-batch-contract.md` (this file)
- `pkg/internal/queue/entryqueue_batch_test.go` (new)
- `pkg/internal/queue/entryqueue.go` only for a bug found (test commit, then
  fix commit).

## Acceptance

- `go test -race -count=3 -run EntryQueueBatch ./pkg/internal/queue/`
- `go vet ./pkg/internal/queue/`
- `make lint`
- `git diff --name-only aff54a08..HEAD` lists only File map files.
