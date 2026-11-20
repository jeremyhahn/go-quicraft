# code-review.yml v5.12.6: restore the LOCAL (security) blocks

Status: BLOCKED. The edit to `.github/workflows/code-review.yml` was refused
by the lane's protected-path guard ("a lane changes it only when its brief
allows it (LANE_POLICY_ALLOW)"). The brief names the file in its File map but
the lane was not launched with that allowance. Relaunch with it; the edits
below are ready to apply as they are.

## The 17 blocks and where each goes in v5.12.6

None is covered by v5.12.6. All 17 are re-applied with their main-branch text.

1. `on:` comment plus `edited` in `pull_request_target.types`.
2. Review job `if:`: `github.event.action != 'edited' &&` with its LOCAL line.
3. Review job `if:`: drop the template's `&& needs.scope.outputs.skip != 'true'`
   and put back the LOCAL comment (TestReviewRunsWhereTheGateNeedsIt).
4. Review job `outputs.skip: ${{ steps.guard.outputs.skip }}`.
5. Review guard: `trap 'rm -f "$WORK/gha.hdr"' EXIT` right after the header
   is written (the review job's own twin wait is unchanged in v5.12.6).
6. Review guard: put the LOCAL comment back in place of the template's
   `rm -f "$WORK/gha.hdr"` after the twin wait.
7. Review guard `*)` case: the reuse block (patch=/ctx= binding plus the
   failed re-review history), unchanged.
8. Opencode job `if:`: `github.event.action != 'edited' &&`.
9. Gate: LOCAL comment above `if: always()`.
10. Gate env `REVIEW_REQUIRED_PATHS`.
11. Gate env `REVIEW_SKIP`.
12. Gate `get_all`: `|| fail` on each page fetch, plus the numeric count check.
13. Gate exemption: `guarded=` and the `[ "$guarded" = 0 ]` term, plus the
    "(LOCAL)" comment and message wording.
14. Gate `live_base_guard()` and its first call, before the exemption exit.
15. Gate: fail on a `failure|cancelled` REVIEW_RESULT, after the
    "changed files" echo.
16. Gate: the second `live_base_guard` call. It now goes after the v5.12.6
    single twin read (the twin wait it used to follow is gone), so it still
    runs on the live PR right before the verdict.
17. Gate: the failed-re-review history and approval patch=/ctx= binding,
    after the twin read and before "(b)". It reads `$wf` from the twin read.
    There is no wait loop. v5.12.6's `twin` fail-at-once check after
    `verdict` stays as it is.

Template values are kept unchanged (header, `runs-on` expressions, the
gate's `timeout-minutes: 10`).

## Checks

Not run, because the file was not changed.
