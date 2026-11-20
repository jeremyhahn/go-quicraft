# code-review.yml v5.12.6: restore the LOCAL (security) blocks

Status: the blocks are applied (17 `LOCAL (security)` markers, the same as
main), and scope and gate run on `runs-on: ubuntu-latest` (a LOCAL
hosted-first override of the template's expression); every check passes,
see Checks.

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
"twin read". Template values are unchanged (header, the gate's `timeout-minutes: 10`)
except `runs-on` on scope and gate: commit 2dd7b2fe replaces the template's
`${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || 'bigdisk' }}`
with `runs-on: ubuntu-latest` under a `# LOCAL (hosted-first)` comment. On
github.com the expression resolves to the same label, so behaviour there is
unchanged; the literal keeps test/buildgates' hosted-first rule
(#1185/#1186), which refuses any `runs-on` expression.

## Checks

- YAML parse (`python3 -c 'import yaml ...'`): not run (PyYAML is not
  installed where the checks ran). actionlint parses the file and passed,
  at 814a2b96 and again at 2dd7b2fe.
- `actionlint .github/workflows/code-review.yml`: pass, no findings.
- `make lint`: pass, 0 issues in every module.
- `go test -count=1 ./test/buildgates/...`: at 814a2b96 FAIL in
  TestCIRunsHostedFirst and TestRunnerProblems (`code-review.yml job scope
  runs on an expression, which can pick a self-hosted label`, and the same
  for job `gate`). At 2dd7b2fe (the hosted-first override above): pass.
  TestReviewRunsWhereTheGateNeedsIt passes at both.
- `make lint`, `make vet`, `go build ./...`, `make lint-gates`,
  `go test -count=1 ./test/releasability/...` and `make coverage
  COVERAGE_THRESHOLD=90`: pass at 2dd7b2fe.
- `git diff --stat 1b208ee3`: only .github/workflows/code-review.yml and
  this plan.
