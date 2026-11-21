# code-review.yml v5.12.6: restore the LOCAL (security) blocks

Status: the blocks are applied (18 `LOCAL (security)` markers: the 17 of
main plus one review fix, block 18), and scope and gate run on `runs-on: ubuntu-latest` (a LOCAL
hosted-first override of the template's expression); every check passes,
see Checks.

## The 18 blocks and where each goes in v5.12.6

None is covered by v5.12.6. Blocks 1-17 are re-applied with their
main-branch text; block 18 is new on this branch (see Review fixes).

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
18. Gate: right after the twin read's `live_base_guard` call (block 16) and
    before block 17, `[ -z "$twin" ] || fail ...`: a queued or in_progress
    twin `Codex review` on the head fails the gate at once, naming the twin
    run, whatever the standing verdict. It answers the P1 "Keep the gate
    closed while a requested re-review is outstanding".

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

## Review fixes

### P1 "Keep the gate closed while a requested re-review is outstanding"

Finding (on 340aeed5): on an approved head, an effort or model label
requests a fresh review through the guard's `again` path. If a title edit
or unrelated label starts another run while that review is queued or
running, that run's review job skips and its gate finds the twin, but the
v5.12.6 APPROVED exception (`[ "$verdict" = APPROVED ] || [ -z "$twin" ]`)
passes on the old verdict; the failed-re-review history only counts
completed jobs.

Change: block 18 above. The gate fails at once for an outstanding twin even
when an older approval stands; that run's gate posts the head's check. The
upstream v5.12.6 line after `verdict` is unchanged (minimal template diff),
and both `runs-on: ubuntu-latest` LOCAL (hosted-first) overrides remain.

Regression: test/buildgates/code_review_gate_twin_test.go extracts the
gate's `fail()`, its twin read through the checks before block 17, and the
verdict block through `case "$verdict"`, and runs them under bash with
stubbed API answers (REVIEW_RESULT=skipped, SCOPE_RESULT=success: the
subsequent edited/labeled event). Cases: approved head with a queued twin
and with an in_progress twin (must fail naming the twin), approved head
with no twin (passes), not approved and changes requested with a twin
(fail as before). It skips when bash or jq is missing and fails if a piece
cannot be found in the workflow.

Checks:

- `go test -count=1 -run TestCodeReviewGateTwin ./test/buildgates/` at the
  test commit (workflow as at 340aeed5): FAIL, the two approved-plus-twin
  cases pass the gate; the other cases pass.
- `grep -c 'LOCAL (security)' .github/workflows/code-review.yml`: 18 (17 at
  340aeed5).
- `actionlint .github/workflows/code-review.yml`: pass, no findings.
- `go test -count=1 ./test/buildgates/...`: pass.
- `make lint`: pass, 0 issues in every module.
- `git diff --stat 340aeed5`: only .github/workflows/code-review.yml,
  test/buildgates/code_review_gate_twin_test.go and this plan.
