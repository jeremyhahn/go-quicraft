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

## Checks

- The diff against the base is exactly the delta (43 lines added, 7 removed).
- `LOCAL (security)` occurs 17 times.
- `go test -count=1 ./test/buildgates/` passes.
- `make lint` passes.
