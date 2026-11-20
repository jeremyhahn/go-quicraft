# Hosted-first runners and their waivers

## The rule

Every workflow job runs on the GitHub-hosted `ubuntu-latest` runner. A job
leaves it only with a waiver on its `runs-on` line that names a category and a
reason:

- `# self-hosted: hardware: <reason>`: the job needs specific hardware.
- `# self-hosted: oversize: <reason>`: the job needs more than a standard
  hosted runner's 4 vCPU, 16 GB memory, about 45 GB disk or 6 hours.
- `# self-hosted: credential: <reason>`: accepted only for the Codex review
  job (`review`) in code-review.yml.

A `runs-on` expression is refused even with a waiver, because it can pick a
self-hosted label. No ci.yml or release.yml job carries a waiver. A job whose
`runs-on` names `release` (ci.yml `publish-images`, off in this repo) is
exempt: the release rule governs it.

## Jobs kept off ubuntu-latest

| Workflow | Job | Runner | Waiver |
| --- | --- | --- | --- |
| code-review.yml | `review` | `codex` | `credential:` the reviewer token is mounted on this runner alone |
| code-review.yml | `review-opencode` | `qwen` | `hardware:` the GPU host that serves the model |

code-review.yml's `scope` and `gate` jobs move from a server-conditional
expression (which could pick `bigdisk`) to `runs-on: ubuntu-latest`, as in the
code-review template's v5.12.4.

## The test

`test/buildgates/ci_runners_test.go`:

- `TestCIRunsHostedFirst` scans every file in `.github/workflows/` and reports
  any job that breaks the rule above, a bare `# self-hosted:` comment, a waiver
  with an empty reason, and any waiver on a ci.yml or release.yml job.
- `TestRunnerProblems` applies each kind of finding to a small workflow as a
  table of mutations, and checks that allowed waivers pass.

## Checks

- `go test -count=1 ./test/buildgates/` fails before the workflow edits
  (`scope` and `gate` run on an expression; `review` and `review-opencode`
  carry no waiver) and passes after them.
- `go vet ./test/buildgates/` and `make lint`.
