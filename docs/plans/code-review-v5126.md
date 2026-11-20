# code-review.yml v5.12.6: restore the LOCAL (security) blocks

Status: the blocks are applied (17 `LOCAL (security)` markers, the same as
main). `test/buildgates` fails on two `runs-on` lines that come from the
template unchanged; see Checks.

## The 17 blocks and where each goes in v5.12.6

None is covered by v5.12.6. All 17 are re-applied with their main-branch text.

1. `on:` comment plus `edited` in `pull_request_target.types`.
2. Review job `if:`: `github.event.action != 'edited' &&` with its LOCAL line.
3. Review job `if:`: the template's `&& needs.scope.outputs.skip != 'true'`
   is dropped and the LOCAL comment is back (TestReviewRunsWhereTheGateNeedsIt).
4. Review job `outputs.skip: ${{ steps.guard.outputs.skip }}`.
5. Review guard: `trap 'rm -f "$WORK/gha.hdr"' EXIT` right after the header
   is written (the review job's own twin wait is unchanged in v5.12.6).
6. Review guard: the LOCAL comment replaces the template's
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
16. Gate: the second `live_base_guard` call. It now follows the v5.12.6
    single twin read (the twin wait it used to follow is gone), so it still
    checks the live PR right before the verdict.
17. Gate: the failed-re-review history and approval patch=/ctx= binding,
    after the twin read and before "(b)". It reads `$wf` from the twin read.
    There is no wait loop. v5.12.6's `twin` fail-at-once check after
    `verdict` stays as it is.

The only wording change inside LOCAL comments in the gate is "twin wait" to
"twin read". Template values are unchanged (header, `runs-on` expressions,
the gate's `timeout-minutes: 10`).

## Checks

- YAML parse (`python3 -c 'import yaml ...'`): not run. The lane's read
  guard refuses an inline script that reads the workflow file. actionlint
  parses the file and passed.
- `actionlint .github/workflows/code-review.yml`: pass, no findings.
- `make lint`: pass, 0 issues in every module.
- `go test -count=1 ./test/buildgates/...`: FAIL in TestCIRunsHostedFirst
  and TestRunnerProblems (all subtests, which re-check the file):
  `code-review.yml job scope runs on an expression, which can pick a
  self-hosted label: "${{ github.server_url == 'https://github.com' &&
  'ubuntu-latest' || 'bigdisk' }}"`, and the same message for job `gate`.
  Both lines are template v5.12.6 verbatim (main had `runs-on:
  ubuntu-latest`). Per the brief, neither the template lines nor the tests
  were changed. The fix needs a decision: either a repository-local
  `runs-on: ubuntu-latest` on scope and gate (main's hosted-first rule,
  #1185/#1186), or a template revision. TestReviewRunsWhereTheGateNeedsIt
  passes.
- `git diff --stat 1b208ee3`: only .github/workflows/code-review.yml and
  this plan.
