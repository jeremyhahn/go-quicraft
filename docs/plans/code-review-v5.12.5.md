# code-review.yml: template v5.12.4 -> v5.12.5

## What changed

`.github/workflows/code-review.yml` takes the template's v5.12.4 -> v5.12.5
delta and nothing else:

- Header version `v5.12.5` and the v5.12.5 changelog entry.
- The guard records the job's start time in `$WORK/job-start`.
- `lib.sh` gains `JOB_BUDGET` (75 minutes in seconds) and `lock_wait NEED`,
  which prints how long a flock on the review lock may wait: what is left of
  the job's budget less the seconds the job still needs once it holds the
  lock. With no time left it fails at once with that reason.
- The login-refresh lock waits `lock_wait 130+360+2730+440` instead of a
  fixed 300 s; the review lock waits `lock_wait limit+30+440` instead of a
  fixed 600 s. Both timeout messages report the computed wait.

Every repository-local hunk is unchanged: the 17 `LOCAL (security)` blocks,
the `runs-on: ubuntu-latest` lines of the scope and gate jobs, and the codex
and qwen runs-on waiver comments.

## Hunks applied by hand

Two comment-only hunks do not apply with `patch` because the local file
differs around them:

1. The header changelog: the v5.12.5 entry goes above the local v5.12.4
   entry, which stays as it is.
2. The budget comment above the codex job's steps: its first two lines are
   replaced as the hunk does.

## Regression tests

`test/buildgates/code_review_lock_wait_test.go` extracts `die()`, the
`JOB_BUDGET=` line, the `lock_wait() { ... }` function and both callers'
reserve expressions from the workflow text (a missing piece fails the test)
and runs them in bash under lib.sh's options (`set -euo pipefail`,
`inherit_errexit`) with `date` stubbed to a fixed epoch.

- `TestCodeReviewLockWait`: a positive budget (elapsed time and the NEED
  reserve are both subtracted); zero and negative budgets, a missing
  `$WORK/job-start` and a malformed one (each dies with its reason on stderr
  and in `$WORK/error`, with no output); the refresh reserve (3660 s) left
  and exhausted; the review reserve at limit 1530 and at limit 2730, left
  and exhausted.
- `TestCodeReviewLockWaitStopsCaller`: `wait_s=$(lock_wait ...)` followed by
  a stand-in for flock; when lock_wait dies the step stops non-zero with the
  reason recorded and the flock stand-in never runs (refresh, review and
  missing start).

Removing `- $1` from a scratch copy of the function fails every budget case
(the reserve is checked). The tests found no defect; the workflow is
unchanged by them.

## Checks

- The diff against the base is exactly the delta (43 lines added, 7 removed).
- `LOCAL (security)` occurs 17 times.
- `go test -count=1 ./test/buildgates/` passes.
- `make lint` passes.
