#!/usr/bin/env python3
"""runner-labels-tests.py -- runs ci.yml's `Runner labels` step as the
workflow runs it and asserts what it passes and fails.

The code under test is read out of .github/workflows/ci.yml, not copied: the
python heredoc of the step named `Runner labels`. The step name and the
heredoc must each be found exactly once, or the test stops with FATAL (exit
9) and measures nothing. Each case writes a repository's .github/workflows/
into a temporary directory and runs the script there with `python3 -I -`, as
the step does; nothing reaches the network.

  A. Passes: this repository's own workflows; a tag-push release.yml on the
     release label; a dispatch on main; the push-or-dispatch guard folded
     over lines (`if: >-`); the runs-on mapping form; a guard written in
     another case; a label that only starts with `release`; a PR workflow
     with no release job.
  B. Fails (a), a pull request can run a release job: no `if:`; an
     expression runs-on that picks `release` on main and another label on a
     PR; a block-list runs-on; pull_request_target with a ref-only guard;
     triggers in a flow mapping.
  C. Fails (b), the `if:` does not restrict the job to the allow-list: a ref
     check with no event check (schedule runs on main); an event check with no
     ref check; a tag-prefix check alone (a dispatch on a tag, a release
     event); RELEASE_RUNNER alone; success(); text around the expression; an
     `if:` that cannot be lexed; a .yaml file is read too. A (b) finding in a
     workflow with no pull request trigger is not also an (a) finding.
  D. GitHub-hosted is the default: on github.com every job that does not
     name `release` runs on a standard GitHub-hosted image (ubuntu-latest,
     ubuntu-24.04, ubuntu-22.04, their -arm forms, ubuntu-slim) on every
     event (c), unless its runs-on line carries `# self-hosted: <reason>`; a
     larger runner (cores, large, xlarge, gpu) is refused even with a waiver
     (d), since it bills on a public repository. A routing expression that
     picks self-hosted for a push to main and a same-repository PR fails
     (c), the server-conditional form passes. Self-hosted is only for
     hardware-specific or oversize jobs: the reason names its category,
     `# self-hosted: hardware: <reason>` or `# self-hosted: oversize:
     <reason>` (`credential:` for the one review job); a waiver with no
     category, or an unknown one, is (e). Label and category are checked
     together: the review-only labels run only in code-review.yml, each with
     its own category; `credential:` belongs to one label alone; any other
     pairing is (f), whatever the waiver says.
  E. In ci.yml a runs-on that reads the preflight's runner selection,
     fromJSON(needs.preflight.outputs.runner) or runner_docker, passes when
     its default gives a standard image and its overflow gives exactly
     [self-hosted, linux, x64, bigdisk] or [..., bigdisk-docker]. The same
     text outside ci.yml, the output without fromJSON, the list written out,
     a self-hosted value on some event, an overflow to another label, and
     another job's output all stay (c).
  F. ci.yml's preflight job runs on the self-hosted bigdisk pool on
     github.com unless the repository variable CI_PREFLIGHT_RUNS_ON overrides
     it. Its runs-on passes unset (exactly [self-hosted, linux, x64,
     bigdisk]) and set to the fallback `"ubuntu-latest"` (a standard image).
     This repository's ci.yml preflight passes (A1 too). The same text in
     another job, outside ci.yml, a default to any other label, a value that
     stays self-hosted with the variable set, and the variable read without
     fromJSON all stay (c). A pull request from a fork never reaches the
     pool: unset, the runs-on must give a standard image for a fork head,
     so the default without the head-repository guard stays (c).
  G. ci-template v1.51: every runs-on is also read under
     pull_request_target, same-repository and fork, the event
     code-review.yml runs on. This repository's code-review.yml passes; a
     label picked only under pull_request_target, or written out, is (c) on
     that event (every event is read in every workflow); in a workflow with
     that trigger a fork's head gets neither the selection's overflow nor
     the unset-variable exemption, and the finding says a fork never reaches
     a self-hosted runner. Under pull_request alone the same text keeps its
     meaning for a same-repository head.
  H. A workflow whose jobs block the validator cannot read fails closed
     (g) and never gets the final OK: a quoted `jobs` key, a flow-style
     jobs mapping, job or workflow, an explicit-key or merge key in a job,
     an aliased runs-on, no jobs key and two jobs keys. The block-style
     form of the same workflow is read as before.
  J. ci-template v1.52: a fork's pull request under pull_request never
     reaches a self-hosted runner. ci.yml's preflight runs-on carries the
     fork clause for every pull_request event, and the lint gives a fork's
     head under pull_request, as under pull_request_target, no
     unset-variable exemption and no overflow; the selection outputs read
     for it are the hosted default. The v1.49 preflight form is (c) for a
     fork's head only; every job of this repository's ci.yml, evaluated with
     the lint's own reader, runs on ubuntu-latest for a fork's pull request
     on github.com, while a same-repository pull request keeps preflight's
     pool default.
  K. ci-template v1.53: code-review.yml's guard jobs may take the
     self-hosted pool for a same-repository head only; a fork's head, another
     label and another workflow stay (c).
  L. With the variable unset, preflight takes the pool only for a push to
     main and a same-repository pull request; every other event is hosted.

Run: python3 -I test/buildgates/testdata/runner-labels-tests.py
"""
import pathlib
import re
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
# LOCAL: this copy lives in test/buildgates/testdata; the workflows it reads are
# the repository's own, in .github/workflows.
WORKFLOWS = HERE.parents[2] / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "ci.yml"
FAILS = 0


def fatal(msg):
    print(f"FATAL: {msg}")
    sys.exit(9)


def script():
    text = WORKFLOW.read_text()
    if text.count("- name: Runner labels\n") != 1:
        fatal("the step `Runner labels` is not in ci.yml exactly once")
    found = re.findall(
        r"- name: Runner labels\n        run: \|\n          python3 -I - <<'PY'\n(.*?)\n          PY\n",
        text, re.S)
    if len(found) != 1:
        fatal("the step's python heredoc is not found exactly once")
    return "\n".join(l[10:] if l.startswith(" " * 10) else l for l in found[0].split("\n")) + "\n"


SCRIPT = script()


def run(files):
    with tempfile.TemporaryDirectory() as d:
        wf = pathlib.Path(d, ".github", "workflows")
        wf.mkdir(parents=True)
        for name, body in files.items():
            (wf / name).write_text(body)
        p = subprocess.run([sys.executable, "-I", "-"], input=SCRIPT, cwd=d,
                           capture_output=True, text=True, timeout=60)
        return p.returncode, p.stdout + p.stderr


def case(label, files, rc, want=(), not_want=()):
    global FAILS
    got, out = run(files)
    ok = got == rc and all(w in out for w in want) and not any(w in out for w in not_want)
    print(f"{'PASS' if ok else 'FAIL'}: {label}")
    if not ok:
        FAILS += 1
        print(f"  exit {got}, wanted {rc}; wanted {list(want)}, not {list(not_want)}")
        print("  " + out.replace("\n", "\n  "))


PR_ON = "on:\n  pull_request:\n    branches: [main]\n  push:\n    branches: [main]\n"
GUARD = "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}"


def job(runs_on, cond=None, name="pub"):
    s = f"  {name}:\n    runs-on: {runs_on}\n"
    if cond is not None:
        s += f"    if: {cond}\n"
    return s + "    steps:\n      - run: echo hi   # if: not a key\n"


def wf(on, *jobs):
    return on + "jobs:\n" + "".join(jobs)


REL = "[self-hosted, linux, x64, release]"

# A. passes
case("A1 this repository's own workflows",
     {p.name: p.read_text() for p in WORKFLOW.parent.glob("*.yml")}, 0, ["OK:"])
case("A2 release.yml: tag push on the release label",
     {"release.yml": wf("on:\n  push:\n    tags: ['v*']\n",
                        job(REL, "startsWith(github.ref, 'refs/tags/') && github.event_name == 'push'"))}, 0)
case("A3 workflow_dispatch on main",
     {"r.yml": wf("on: [workflow_dispatch]\n",
                  job(REL, "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main'"))}, 0)
case("A4 push-or-dispatch guard folded over lines, beside a PR trigger",
     {"ci.yml": wf(PR_ON, job(REL, ">-\n      ${{ (github.event_name == 'push' &&\n"
                                   "      (github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/')))\n"
                                   "      || (github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main') }}"),
                   job("ubuntu-latest", name="test"))}, 0)
case("A5 runs-on mapping form, guarded, with RELEASE_RUNNER",
     {"ci.yml": wf(PR_ON, "  pub:\n    runs-on:\n      group: rel\n      labels: [self-hosted, release]\n"
                          "    if: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && vars.RELEASE_RUNNER == 'true' }}\n"
                          "    steps:\n      - run: true\n")}, 0)
case("A6 comparison is case-insensitive, as on the forge",
     {"ci.yml": wf(PR_ON, job(REL, "\"${{ github.event_name == 'PUSH' && github.ref == 'refs/heads/main' }}\""))}, 0)
case("A7 a label that only starts with release is not release",
     {"ci.yml": wf(PR_ON, job("[self-hosted, release-notes]  # self-hosted: hardware: the notes host"))}, 0)
case("A8 a PR workflow with no release job",
     {"ci.yml": wf(PR_ON, job("${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || 'bigdisk' }}"))}, 0)

# B. fails (a)
case("B1 PR workflow, release job, no if",
     {"ci.yml": wf(PR_ON, job(REL))}, 1,
     [".github/workflows/ci.yml:pub: (a)", ".github/workflows/ci.yml:pub: (b)"])
case("B2 expression runs-on: release on main, bigdisk on a PR (go-xkms ci-images)",
     {"ci.yml": wf(PR_ON, job("${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || "
                              "(github.event_name == 'push' && github.ref == 'refs/heads/main') && 'release' || 'bigdisk' }}",
                              "${{ !github.event.pull_request.draft }}", name="ci-images"))}, 1,
     ["ci.yml:ci-images: (a) names `release` and a pull_request event can run it"])
case("B3 block-list runs-on",
     {"ci.yml": wf(PR_ON, "  pub:\n    runs-on:\n      - self-hosted\n      - release   # the release pool\n"
                          "    steps:\n      - run: true\n")}, 1, ["ci.yml:pub: (a)"])
case("B4 pull_request_target with a ref-only guard (its ref is the base branch)",
     {"t.yml": wf("on:\n  pull_request_target:\n", job(REL, "github.ref == 'refs/heads/main'"))}, 1,
     ["t.yml:pub: (a) names `release` and a pull_request_target event can run it"])
case("B5 triggers in a flow mapping",
     {"ci.yml": wf("on: {push: {branches: [main]}, pull_request: {}}\n", job(REL, "github.ref == 'refs/heads/main'"))}, 1,
     ["ci.yml:pub: (b)"], ["(a)"])  # pull_request's ref is refs/pull/N/merge: only (b)

# C. fails (b)
case("C1 ref check with no event check: schedule on main runs it; no PR trigger, so no (a)",
     {"r.yml": wf("on:\n  push:\n  schedule:\n    - cron: '0 4 * * *'\n", job(REL, "github.ref == 'refs/heads/main'"))}, 1,
     ["r.yml:pub: (b)", "schedule"], ["(a)"])
case("C2 event check with no ref check",
     {"r.yml": wf("on: push\n", job(REL, "${{ github.event_name == 'push' }}"))}, 1,
     ["r.yml:pub: (b)", "push (refs/heads/feature)"])
case("C3 tag-prefix check alone: a dispatch on a tag and a release event",
     {"r.yml": wf("on: [push, workflow_dispatch]\n", job(REL, "startsWith(github.ref, 'refs/tags/')"))}, 1,
     ["workflow_dispatch (refs/tags/v1.0.0)", "release (refs/tags/v1.0.0)"])
case("C4 RELEASE_RUNNER alone",
     {"ci.yml": wf(PR_ON, job(REL, "vars.RELEASE_RUNNER == 'true'"))}, 1, ["ci.yml:pub: (a)", "ci.yml:pub: (b)"])
case("C5 success() alone",
     {"r.yml": wf("on: push\n", job(REL, "success()"))}, 1, ["r.yml:pub: (b)"])
case("C6 text around the expression is a string, always true",
     {"r.yml": wf("on: push\n", job(REL, "\"push: " + GUARD + "\""))}, 1, ["r.yml:pub: (b)", "cannot be read"])
case("C7 an if that cannot be lexed",
     {"r.yml": wf("on: push\n", job(REL, "github.ref == \"refs/heads/main\""))}, 1, ["r.yml:pub: (b)", "cannot be read"])
case("C8 a .yaml file is read too",
     {"ok.yml": wf("on: push\n", job("ubuntu-latest")), "rel.yaml": wf("on: push\n", job(REL))}, 1,
     ["rel.yaml:pub: (b)"], ["ok.yml"])
case("C9 two parsed arrays are never the same instance, so != is true on a pull request",
     {"ci.yml": wf(PR_ON, job(REL, "${{ fromJSON('[]') != fromJSON('[]') }}"))}, 1, ["ci.yml:pub: (a)"])
case("C10 two parsed objects are never the same instance, so != is true on a pull request",
     {"ci.yml": wf(PR_ON, job(REL, "${{ fromJSON('{}') != fromJSON('{}') }}"))}, 1, ["ci.yml:pub: (a)"])
case("C11 relational binds tighter than equality: !(0 == (1 < 2)) is true on a pull request",
     {"ci.yml": wf(PR_ON, job(REL, "${{ !(0 == 1 < 2) }}"))}, 1, ["ci.yml:pub: (a)"])
case("C12 a whole number converts to its digits: contains(1, '1.0') is false, so its negation is true",
     {"ci.yml": wf(PR_ON, job(REL, "${{ !contains(1, '1.0') }}"))}, 1, ["ci.yml:pub: (a)"])
case("C13 a relational guard is still evaluated: false == (1 < 2) and 1 < 2 == false are false, never runnable",
     {"ci.yml": wf(PR_ON, job(REL, "${{ false == 1 < 2 }}"), job(REL, "${{ 1 < 2 == false }}", name="pub2"))}, 0, ["OK:"])
case("C14 format's doubled braces are literal: format('{{0}}', 'x') is '{0}', so == '{0}' is true",
     {"ci.yml": wf(PR_ON, job(REL, "${{ format('{{0}}', 'x') == '{0}' }}"))}, 1, ["ci.yml:pub: (a)"])
case("C15 a format placeholder past the arguments is an error, never provably false",
     {"ci.yml": wf(PR_ON, job(REL, "${{ format('{1}', 'x') == 'y' }}"))}, 1, ["ci.yml:pub: (a)"])
case("C16 a stray format brace is an error, never provably false",
     {"ci.yml": wf(PR_ON, job(REL, "${{ format('{0', 'x') == 'y' }}"))}, 1, ["ci.yml:pub: (a)"])
case("C17 a push to a branch the samples do not name still runs it",
     {"r.yml": wf("on: push\n", job(REL, "github.event_name == 'push' && github.ref == 'refs/heads/develop'"))}, 1,
     ["r.yml:pub: (b)", "push (refs/heads/feature)"])
case("C18 a ref prefix the samples do not hit still runs it",
     {"r.yml": wf("on: push\n", job(REL, "github.event_name == 'push' && startsWith(github.ref, 'refs/heads/rel')"))}, 1,
     ["r.yml:pub: (b)"])
case("C19 an event the samples do not list still runs it",
     {"r.yml": wf("on: [push, deployment]\n", job(REL, "github.event_name == 'deployment'"))}, 1,
     ["r.yml:pub: (b)", "another event"])
case("C20 GitHub reads '1_0' as NaN, so '1_0' != 10 is true on a pull request",
     {"ci.yml": wf(PR_ON, job(REL, "${{ '1_0' != 10 }}"))}, 1, ["ci.yml:pub: (a)"])
case("C21 pull_request_target's ref is its base branch, which need not be main",
     {"t.yml": wf("on:\n  pull_request_target:\n",
                  job(REL, "github.event_name == 'pull_request_target' && github.ref != 'refs/heads/main'"))}, 1,
     ["t.yml:pub: (a)"])
case("C22 a pull request's base branch is not known",
     {"ci.yml": wf(PR_ON, job(REL, "github.event_name == 'pull_request' && github.base_ref == 'develop'"))}, 1,
     ["ci.yml:pub: (a)"])
case("C23 a release on a branch named by its ref name only",
     {"r.yml": wf("on: push\n", job(REL, "github.event_name == 'push' && github.ref_name == 'develop'"))}, 1,
     ["r.yml:pub: (b)"])
case("C24 a guard that names every sampled event but one still runs on an unlisted event",
     {"r.yml": wf("on: push\n", job(REL, "${{ !contains(fromJSON('[\"push\", \"pull_request\", \"pull_request_target\", "
                                         "\"workflow_dispatch\", \"schedule\", \"workflow_run\", \"merge_group\", \"release\", "
                                         "\"create\", \"issue_comment\", \"repository_dispatch\"]'), github.event_name) }}"))}, 1,
     ["r.yml:pub: (b)", "another event"])
case("C25 contains over a parsed array still runs on a branch the samples do not name",
     {"r.yml": wf("on: push\n", job(REL, "${{ contains(fromJSON('[\"develop\"]'), github.ref_name) }}"))}, 1,
     ["r.yml:pub: (b)"])
case("C26 a create event's github.event.ref is the short name, so it may be develop",
     {"r.yml": wf("on: [push, create]\n",
                  job(REL, "github.event_name == 'create' && github.event.ref == 'develop'"))}, 1,
     ["r.yml:pub: (b)"])
case("C27 runner labels are case-insensitive: a waived RELEASE scalar in a PR workflow is still release",
     {"aux.yml": wf(PR_ON, job("RELEASE  # self-hosted: hardware: dedicated host"))}, 1,
     ["aux.yml:pub: (a)"])
case("C28 a mixed-case release label in a list is still release",
     {"aux.yml": wf(PR_ON, job("[self-hosted, linux, x64, Release]  # self-hosted: hardware: dedicated host"))}, 1,
     ["aux.yml:pub: (a)"])
case("C29 a tag may be named main, so ref_name == 'main' on a dispatch does not prove the main branch",
     {"r.yml": wf("on: [workflow_dispatch]\n",
                  job(REL, "github.event_name == 'workflow_dispatch' && github.ref_name == 'main'"))}, 1,
     ["r.yml:pub: (b)"])
case("A9 a main-only push guard passes, with unlisted events in the triggers",
     {"r.yml": wf("on: [push, workflow_dispatch, deployment]\n",
                  job(REL, "github.ref == 'refs/heads/main' && github.event_name == 'push'"))}, 0, ["OK:"])

# D. hosted first (v1.43)
ROUTE = ("${{ ((github.event_name == 'push' && github.ref == 'refs/heads/main') || "
         "(github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository)) "
         "&& fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk-docker\"]') || 'ubuntu-latest' }}")
case("D1 ubuntu-24.04, ubuntu-latest-arm-free forms and ubuntu-slim pass",
     {"ci.yml": wf(PR_ON, job("ubuntu-24.04"), job("ubuntu-24.04-arm", name="arm"), job("ubuntu-slim", name="slim"))}, 0)
case("D2 the server-conditional form passes",
     {"ci.yml": wf(PR_ON, job("${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || 'ci-gate' }}"))}, 0)
case("D3 the P1 routing expression fails (c): self-hosted on a push to main and a same-repository PR",
     {"ci.yml": wf(PR_ON, job(ROUTE, name="lint"))}, 1,
     ["ci.yml:lint: (c)", "push (refs/heads/main)"])
case("D4 the same expression with an oversize reason passes",
     {"ci.yml": wf(PR_ON, job(ROUTE + "  # self-hosted: oversize: 24 GB stage memory (#2063)", name="image"))}, 0)
case("D5 a plain self-hosted list fails (c)",
     {"ci.yml": wf(PR_ON, job("[self-hosted, linux, x64, bigdisk]", name="unit"))}, 1, ["ci.yml:unit: (c)"])
case("D6 a bare waiver with no reason fails (e)",
     {"ci.yml": wf(PR_ON, job("[self-hosted, bigdisk]  # self-hosted:", name="unit"))}, 1, ["ci.yml:unit: (e)"])
case("D7 a larger runner fails (d), even with a waiver",
     {"ci.yml": wf(PR_ON, job("ubuntu-latest-8-cores  # self-hosted: faster", name="big"))}, 1, ["ci.yml:big: (d)"])
case("D8 a runs-on read from a job output fails (c) unless waived",
     {"ci.yml": wf(PR_ON, job("${{ needs.route.outputs.label }}", name="dyn"))}, 1, ["ci.yml:dyn: (c)"])
case("D9 a block-list self-hosted runs-on with the waiver on its key line passes",
     {"ci.yml": wf(PR_ON, "  hw:\n    runs-on:  # self-hosted: hardware: the TPM bench\n      - self-hosted\n      - tpm\n"
                          "    steps:\n      - run: true\n")}, 0)
case("D11 a v1.43 reason with no category fails (e)",
     {"ci.yml": wf(PR_ON, job("[self-hosted, bigdisk]  # self-hosted: 24 GB stage memory", name="image"))}, 1,
     ["ci.yml:image: (e)"], ["(c)"])
case("D12 an unknown category fails (e)",
     {"ci.yml": wf(PR_ON, job("[self-hosted, bigdisk]  # self-hosted: faster: the hosted runner is slow", name="unit"))}, 1,
     ["ci.yml:unit: (e)"])
case("D13 codex with the credential category passes in code-review.yml",
     {"code-review.yml": wf(PR_ON, job("codex  # self-hosted: credential: the reviewer token is on this runner alone", name="codex"))}, 0)
case("D15 codex with a hardware waiver outside code-review.yml fails (f) (go-qrdb #1642)",
     {"ci.yml": wf(PR_ON, job("codex  # self-hosted: hardware: the review host", name="unit"))}, 1,
     ["ci.yml:unit: (f)", "codex"])
case("D16 codex with the credential category outside code-review.yml fails (f)",
     {"ci.yml": wf(PR_ON, job("codex  # self-hosted: credential: the reviewer token is on this runner alone", name="codex"))}, 1,
     ["ci.yml:codex: (f)"])
case("D17 qwen in a list, with an oversize waiver, outside code-review.yml fails (f)",
     {"batch.yml": wf(PR_ON, job("[self-hosted, qwen]  # self-hosted: oversize: big model", name="bench"))}, 1,
     ["batch.yml:bench: (f)", "qwen"])
case("D18 codex picked by an expression outside code-review.yml fails (f)",
     {"ci.yml": wf(PR_ON, job("${{ github.server_url == 'https://github.com' && 'codex' || 'ci-gate' }}  # self-hosted: hardware: x", name="dyn"))}, 1,
     ["ci.yml:dyn: (f)"])
case("D19 codex with hardware: in code-review.yml fails (f): the wrong category",
     {"code-review.yml": wf(PR_ON, job("codex  # self-hosted: hardware: the review host", name="codex"))}, 1,
     ["code-review.yml:codex: (f)", "hardware"])
case("D20 qwen with credential: in code-review.yml fails (f)",
     {"code-review.yml": wf(PR_ON, job("qwen  # self-hosted: credential: a key", name="oc"))}, 1,
     ["code-review.yml:oc: (f)"])
case("D21 qwen with hardware: in code-review.yml passes",
     {"code-review.yml": wf(PR_ON, job("qwen  # self-hosted: hardware: the GPU host that serves the model", name="oc"))}, 0)
case("D22 credential: on a non-review label fails (f), in any file",
     {"code-review.yml": wf(PR_ON, job("[self-hosted, bigdisk]  # self-hosted: credential: a token", name="x"))}, 1,
     ["code-review.yml:x: (f)", "credential"])
case("D23 a label that only contains codex is not the review label",
     {"ci.yml": wf(PR_ON, job("[self-hosted, codex-cache]  # self-hosted: hardware: the cache disk", name="c"))}, 0, [], ["(f)"])
case("D24 codex named in the reason text only is not the label",
     {"ci.yml": wf(PR_ON, job("[self-hosted, bigdisk]  # self-hosted: oversize: shares the codex host disk", name="c"))}, 0, [], ["(f)"])
case("D14 a space before the category colon passes",
     {"ci.yml": wf(PR_ON, job("[self-hosted, gpu-pool]  # self-hosted: hardware : the GPU", name="gpu"))}, 0)  # LOCAL: a pool label, not a host name
case("D10 a release job is the release rule's, not (c)'s",
     {"release.yml": wf("on:\n  push:\n    tags: ['v*']\n",
                        job(REL, "startsWith(github.ref, 'refs/tags/') && github.event_name == 'push'"))}, 0, [], ["(c)"])

# E. the runner selection's overflow
SEL = "${{ github.server_url == 'https://github.com' && fromJSON(needs.preflight.outputs.runner_docker) || 'bigdisk' }}"
SEL_PLAIN = "${{ github.server_url == 'https://github.com' && fromJSON(needs.preflight.outputs.runner) || 'bigdisk' }}"
SEL_GATE = ("${{ github.server_url == 'https://github.com' && fromJSON(needs.preflight.outputs.runner || "
            "'\"ubuntu-latest\"') || 'ci-gate' }}")
case("E1 ci.yml: the selection outputs, both classes and CI Gate's guarded form, pass",
     {"ci.yml": wf(PR_ON, job(SEL, name="image"), job(SEL_PLAIN, name="plain"), job(SEL_GATE, name="gate"))}, 0)
case("E2 the selection output outside ci.yml fails (c)",
     {"batch.yml": wf(PR_ON, job(SEL, name="image"))}, 1, ["batch.yml:image: (c)"])
case("E3 the output without fromJSON fails (c): its default is not an image name",
     {"ci.yml": wf(PR_ON, job("${{ needs.preflight.outputs.runner }}", name="raw"))}, 1, ["ci.yml:raw: (c)"])
case("E4 the overflow list written out, with no waiver, still fails (c)",
     {"ci.yml": wf(PR_ON, job("[self-hosted, linux, x64, bigdisk-docker]", name="unit"))}, 1, ["ci.yml:unit: (c)"])
case("E5 self-hosted on some event beside the selection fails (c)",
     {"ci.yml": wf(PR_ON, job("${{ github.event_name == 'push' && fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') "
                              "|| fromJSON(needs.preflight.outputs.runner) }}", name="mix"))}, 1,
     ["ci.yml:mix: (c)", "push (refs/heads/main)"])
case("E6 an overflow to any other label fails (c)",
     {"ci.yml": wf(PR_ON, job("${{ needs.preflight.outputs.runner_docker == '\"ubuntu-latest\"' && 'ubuntu-latest' || 'other-pool' }}",
                              name="other"))}, 1, ["ci.yml:other: (c)", "runner selection overflow"])
case("E7 another job's output is not the selection: (c)",
     {"ci.yml": wf(PR_ON, job("${{ fromJSON(needs.route.outputs.runner) }}", name="dyn"))}, 1, ["ci.yml:dyn: (c)"])

# F. preflight on the self-hosted bigdisk pool by default, the variable its fallback
PRE_UNGUARDED = ("${{ github.server_url == 'https://github.com' && fromJSON(vars.CI_PREFLIGHT_RUNS_ON || "
                 "'[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || 'ubuntu-latest' }}")
# v1.52, as v1.59 writes it: the pool only for a push to main and a
# same-repository pull_request, before the variable.
PRE = PRE_UNGUARDED.replace("&& fromJSON(vars.", "&& ((github.event_name == 'push' && github.ref == 'refs/heads/main') || "
                            "(github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository)) "
                            "&& fromJSON(vars.")
PRE_TEXT = (WORKFLOWS / "ci.yml").read_text()
if PRE_TEXT.count(f"    runs-on: {PRE}   # v1.52\n") != 1:
    fatal("ci.yml's preflight runs-on is not the form part F tests, exactly once")
case("F1 ci.yml: preflight's runs-on passes, unset and set to the fallback",
     {"ci.yml": wf(PR_ON, job(PRE, name="preflight"))}, 0, ["OK:"])
case("F2 the same runs-on on another ci.yml job fails (c): the variable is preflight's alone",
     {"ci.yml": wf(PR_ON, job(PRE, name="lint"))}, 1, ["ci.yml:lint: (c)", "an unknown label"], ["CI_PREFLIGHT_RUNS_ON"])
case("F3 the same runs-on outside ci.yml fails (c)",
     {"batch.yml": wf(PR_ON, job(PRE, name="preflight"))}, 1, ["batch.yml:preflight: (c)"])
case("F4 a default to any other self-hosted label fails (c)",
     {"ci.yml": wf(PR_ON, job(PRE.replace('\\"bigdisk\\"', '\\"bigdisk-docker\\"').replace('"bigdisk"]', '"bigdisk-docker"]'),
                              name="preflight"))}, 1, ["ci.yml:preflight: (c)", "bigdisk-docker", "CI_PREFLIGHT_RUNS_ON unset"])
case("F5 self-hosted on some event whatever the variable holds fails (c), set",
     {"ci.yml": wf(PR_ON, job("${{ github.event_name == 'push' && fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') "
                              "|| fromJSON(vars.CI_PREFLIGHT_RUNS_ON || '\"ubuntu-latest\"') }}", name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", 'CI_PREFLIGHT_RUNS_ON set to "ubuntu-latest"', "push (refs/heads/main"])
case("F6 the variable without fromJSON fails (c): set, it is not an image name",
     {"ci.yml": wf(PR_ON, job("${{ vars.CI_PREFLIGHT_RUNS_ON || 'ubuntu-latest' }}", name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", 'CI_PREFLIGHT_RUNS_ON set to "ubuntu-latest"'])
case("F7 another variable is not the switch: (c)",
     {"ci.yml": wf(PR_ON, job(PRE.replace("CI_PREFLIGHT_RUNS_ON", "CI_PREFLIGHT_RUNS_ON_X"), name="preflight"))}, 1,
     ["ci.yml:preflight: (c)"])
case("F8 the fallback also serves a fork pull request on github.com: a self-hosted fallback fails (c)",
     {"ci.yml": wf(PR_ON, job(PRE.replace("|| 'ubuntu-latest'", "|| 'bigdisk'"), name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", "pull_request (refs/pull/1/merge, fork, CI_PREFLIGHT_RUNS_ON unset, "
      "pull_request: a fork never reaches a self-hosted runner): bigdisk"],
     ["push (refs/heads/main"])
case("F9 the default without the head-repository guard fails (c): a fork pull request would reach the pool",
     {"ci.yml": wf(PR_ON, job(PRE_UNGUARDED, name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", "pull_request (refs/pull/1/merge, fork, CI_PREFLIGHT_RUNS_ON unset, "
      "pull_request: a fork never reaches a self-hosted runner): self-hosted"],
     ["push (refs/heads/main"])
case("F10 a guard that keeps every pull request off the pool passes",
     {"ci.yml": wf(PR_ON, job(PRE.replace(" || github.event.pull_request.head.repo.full_name == github.repository", ""),
                              name="preflight"))}, 0, ["OK:"])

# G. pull_request_target, same-repository and fork (ci-template v1.51)
PRT_ON = ("on:\n  pull_request_target:\n    branches: [main]\n    types: [opened, synchronize]\n"
          "  push:\n    branches: [main]\n")
PRT_ONLY = "${{ github.event_name == 'pull_request_target' && 'bigdisk' || 'ubuntu-latest' }}"
FORK_NOTE = "pull_request_target: a fork never reaches a self-hosted runner"
case("G1 code-review.yml: this repository's workflow passes alone",
     {"code-review.yml": (WORKFLOW.parent / "code-review.yml").read_text()}, 0, ["OK:"])
case("G2 a label picked only under pull_request_target fails (c) on that event, both heads",
     {"signatures.yml": wf(PRT_ON, job(PRT_ONLY, name="signatures"))}, 1,
     ["signatures.yml:signatures: (c)", "pull_request_target (refs/heads/main): bigdisk",
      f"pull_request_target (refs/heads/main, fork, {FORK_NOTE}): bigdisk"])
case("G3 the same text fails without the trigger too, as every event is read in every workflow; no fork note",
     {"signatures.yml": wf(PR_ON, job(PRT_ONLY, name="signatures"))}, 1,
     ["signatures.yml:signatures: (c)", "pull_request_target (refs/heads/main, fork): bigdisk"], [FORK_NOTE])
case("F11 the off-github.com label is not checked: the forge keeps ubuntu-latest",
     {"ci.yml": wf(PR_ON, job(PRE.replace("|| 'ubuntu-latest' }}",  # v1.52: that tail is also a fork's
                                          "|| github.server_url != 'https://github.com' && 'bigdisk' || 'ubuntu-latest' }}"),
                              name="preflight"))}, 0, ["OK:"])
case("G4 a hard-coded self-hosted label under pull_request_target fails (c) on both heads",
     {"signatures.yml": wf(PRT_ON, job("[self-hosted, linux, x64, bigdisk]", name="signatures"))}, 1,
     ["signatures.yml:signatures: (c)", "pull_request_target (refs/heads/main):",
      "pull_request_target (refs/heads/main, fork"])
case("G5 ci.yml with pull_request_target: the unguarded selection gets no overflow for a fork's head",
     {"ci.yml": wf(PRT_ON, job(SEL_PLAIN, name="lint"))}, 1,
     ["ci.yml:lint: (c)", f"pull_request_target (refs/heads/main, fork, runner selection overflow, {FORK_NOTE})"])
case("G6 ci.yml under pull_request: the same selection keeps its overflow (same-repository head)",
     {"ci.yml": wf(PR_ON, job(SEL_PLAIN, name="lint"))}, 0, ["OK:"])
case("G7 ci.yml with pull_request_target: preflight's unset default is no exemption for a fork's head",
     {"ci.yml": wf(PRT_ON, job(PRE_UNGUARDED, name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", f"fork, CI_PREFLIGHT_RUNS_ON unset, {FORK_NOTE}"])
case("G8 a waived self-hosted job under pull_request_target is still the waiver's",
     {"signatures.yml": wf(PRT_ON, job("[self-hosted, linux, x64, bigdisk]  # self-hosted: oversize: test",
                                       name="signatures"))}, 0, ["OK:"])

# H. A jobs block the validator cannot read fails closed (g), never OK.
QWEN = "qwen  # self-hosted: hardware: GPU"
AUX = ("on:\n  pull_request:\n    branches: [main]\n"
       "jobs:\n  aux:\n    runs-on: " + QWEN + "\n    steps:\n      - run: echo hi\n")
for label, text in (
        ("H1 a double-quoted \"jobs\": key", AUX.replace("jobs:\n", '"jobs":\n')),
        ("H2 a single-quoted 'jobs': key", AUX.replace("jobs:\n", "'jobs':\n")),
        ("H3 a flow-style jobs mapping",
         "on:\n  pull_request:\n    branches: [main]\n"
         "jobs: {aux: {runs-on: qwen, steps: [{run: echo hi}]}}  # self-hosted: hardware: GPU\n"),
        ("H4 a job written as a flow mapping",
         "on:\n  pull_request:\n    branches: [main]\n"
         "jobs:\n  aux: {runs-on: qwen, steps: [{run: echo hi}]}  # self-hosted: hardware: GPU\n"),
        ("H5 a whole flow-style workflow",
         "{on: {pull_request: {branches: [main]}}, jobs: {aux: {runs-on: qwen, steps: [{run: echo hi}]}}}\n"),
        ("H6 a job key in an explicit-key form",
         AUX.replace("    runs-on: " + QWEN, "    ? runs-on\n    : " + QWEN)),
        ("H7 a merge key in a job",
         "on:\n  pull_request:\n    branches: [main]\n"
         "x-base: &base\n  runs-on: qwen\n"
         "jobs:\n  aux:\n    <<: *base  # self-hosted: hardware: GPU\n    steps:\n      - run: echo hi\n"),
        ("H8 an aliased runs-on",
         "on:\n  pull_request:\n    branches: [main]\n"
         "env:\n  R: &r qwen\n"
         "jobs:\n  aux:\n    runs-on: *r  # self-hosted: hardware: GPU\n    steps:\n      - run: echo hi\n"),
        ("H9 no jobs key at all", "on:\n  pull_request:\n    branches: [main]\n"),
        ("H10 two jobs keys", AUX + "jobs:\n  other:\n    runs-on: ubuntu-latest\n"),
):
    case(f"{label}: (g), not OK", {"aux.yml": text}, 1, ["aux.yml", "(g)"], ["OK:"])
case("H11 the block-style form of the same workflow is read: (f) for qwen outside code-review.yml",
     {"aux.yml": AUX}, 1, ["aux.yml:aux: (f)"], ["(g)", "OK:"])

# I. `a || b` and `a && b` with an unknown left operand stay unknown: the left
# operand's value, not the right one, is the result whenever it is truthy (||)
# or falsy (&&).
case("I1 an unknown variable before a hosted fallback is not the fallback",
     {"ci.yml": wf(PR_ON, job("${{ vars.RUNNER || 'ubuntu-latest' }}"))}, 1, ["ci.yml:pub:"], ["OK:"])
case("I2 an unknown variable before a hosted fallback is not the fallback, under &&",
     {"ci.yml": wf(PR_ON, job("${{ vars.RUNNER && 'ubuntu-latest' }}"))}, 1, ["ci.yml:pub:"], ["OK:"])
case("I3 a release job guarded by an unknown variable's fallback can run on a pull request",
     {"ci.yml": wf(PR_ON, job(REL, "${{ (vars.ALLOW_RELEASE || 'no') == 'yes' }}"))}, 1, ["ci.yml:pub:"], ["OK:"])
case("I4 a release job guarded by an unknown variable under && can run on a pull request",
     {"ci.yml": wf(PR_ON, job(REL, "${{ (vars.ALLOW_RELEASE && 'no') == 'no' }}"))}, 1, ["ci.yml:pub:"], ["OK:"])
case("I5 literal operands still evaluate exactly: 'ubuntu-latest' || 'bigdisk'",
     {"ci.yml": wf(PR_ON, job("${{ 'ubuntu-latest' || 'bigdisk' }}"))}, 0, ["OK:"])
case("I6 literal operands still evaluate exactly: '' || 'ubuntu-latest'",
     {"ci.yml": wf(PR_ON, job("${{ '' || 'ubuntu-latest' }}"))}, 0, ["OK:"])
case("I7 literal operands still evaluate exactly: 'bigdisk' && 'ubuntu-latest'",
     {"ci.yml": wf(PR_ON, job("${{ 'bigdisk' && 'ubuntu-latest' }}"))}, 0, ["OK:"])
case("I8 an unknown variable cannot open a release guard that is false on its own",
     {"ci.yml": wf(PR_ON, job(REL, GUARD.replace(" }}", " && (vars.ALLOW_RELEASE || 'no') == 'yes' }}")))}, 0, ["OK:"])

# J. a fork's pull request under pull_request (ci-template v1.52)
FORK_PR_NOTE = ("pull_request (refs/pull/1/merge, fork, CI_PREFLIGHT_RUNS_ON unset, "
                "pull_request: a fork never reaches a self-hosted runner)")
case("J1 ci.yml: the v1.49 preflight runs-on fails (c) for a fork's head under pull_request",
     {"ci.yml": wf(PR_ON, job(PRE_UNGUARDED, name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", FORK_PR_NOTE + ": self-hosted, linux, x64, bigdisk"],
     ["pull_request (refs/pull/1/merge, CI_PREFLIGHT_RUNS_ON unset", "push (refs/heads/main"])
case("J2 ci.yml: the v1.52 preflight runs-on passes under pull_request and pull_request_target",
     {"ci.yml": wf(PR_ON.replace("on:\n", "on:\n  pull_request_target:\n    branches: [main]\n"),
                   job(PRE, name="preflight"))}, 0, ["OK:"])
CI_TEXT = WORKFLOW.read_text()
case("J3 this repository's ci.yml with preflight's v1.49 runs-on fails (c), for the fork alone",
     {"ci.yml": CI_TEXT.replace(f"    runs-on: {PRE}   # v1.52\n", f"    runs-on: {PRE_UNGUARDED}   # v1.49\n")}, 1,
     ["ci.yml:preflight: (c)", FORK_PR_NOTE], ["ci.yml:lint:", "ci.yml:image:", "ci.yml:ci-gate:"])
case("J4 a selection job that routes a fork's head to the pool itself fails (c) under pull_request",
     {"ci.yml": wf(PR_ON, job("${{ github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name != github.repository && "
                              "fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || "
                              "fromJSON(needs.preflight.outputs.runner) }}", name="lint"))}, 1,
     ["ci.yml:lint: (c)", "pull_request (refs/pull/1/merge, fork, runner selection overflow, pull_request: a fork never reaches"],
     ["push (refs/heads/main", "pull_request (refs/pull/1/merge):"])


def every_job(head):
    # Each job's runs-on in this repository's ci.yml, read by the lint's own
    # reader on github.com under pull_request, in every state of its inputs
    # it is checked in.
    ns = {}
    exec(SCRIPT.split("\nfiles = sorted(", 1)[0], ns)
    lines = CI_TEXT.splitlines()
    got = {}
    for i, s in enumerate(lines):
        if re.match(r"jobs\s*:\s*(#.*)?$", s):
            for j, name, _ in ns["keys_under"](lines, i):
                for k, key, inline in ns["keys_under"](lines, j):
                    if key == "runs-on":
                        ro = ns["body"](lines, k, inline)
                        if ns["names_release"](ro):
                            continue
                        sels = ("unset", "set") if name == "preflight" else ("default", "overflow")
                        got[name] = {sel: ns["runs_on_labels"](ro, "pull_request", "refs/pull/1/merge", head, sel)
                                     for sel in sels}
    return got


# LOCAL: the jobs the template gives the selection (preflight its own default),
# and this repository's toolchain-image legs that take it as integration does.
# Its other ci.yml jobs stay on ubuntu-latest on every event.
SELECTION_JOBS = {"preflight", "row", "lint", "image", "lint-gates", "integration", "soak", "ci-gate",
                  "unit", "coverage", "netfault", "errfs", "zero-rtt", "cross-arch", "modules",
                  "contrib-dragonboat", "fips", "clusterproof"}  # LOCAL (#1302): clusterproof


def selection_case(label, head, want):
    global FAILS
    got = every_job(head)
    bad = {n: v for n, v in got.items() if any(want(n, sel, l) is False for sel, l in v.items())}
    ok = len(got) >= 6 and SELECTION_JOBS <= set(got) and not bad
    print(f"{'PASS' if ok else 'FAIL'}: {label}")
    if not ok:
        FAILS += 1
        print(f"  {len(got)} job(s) read; off: {bad or got}")


selection_case("J5 this repository's ci.yml: every job runs on ubuntu-latest for a fork's pull request on github.com",
               "fork/r", lambda n, sel, l: l == ["ubuntu-latest"])
selection_case("J6 this repository's ci.yml: a same-repository pull request keeps the pool default and overflow",
               "o/r", lambda n, sel, l: l == (["ubuntu-latest"] if sel in ("set", "default") or n not in SELECTION_JOBS else
                                             ["self-hosted", "linux", "x64", "bigdisk"] if n in ("preflight", "ci-gate", "row")
                                             else ["self-hosted", "linux", "x64", "bigdisk-docker"]))

# K. code-review.yml's guard jobs on the self-hosted pool for a same-repository head (v1.53)
CR_ON = "on:\n  pull_request_target:\n    branches: [main]\n    types: [opened, synchronize]\n"
SAME = ("${{ github.server_url == 'https://github.com' && (github.event.pull_request.head.repo.full_name == github.repository "
        "&& fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || 'ubuntu-latest') || 'bigdisk' }}")
ANYHEAD = ("${{ github.server_url == 'https://github.com' && (github.event_name == 'pull_request_target' "
           "&& fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || 'ubuntu-latest') || 'bigdisk' }}")
case("K1 code-review.yml: a guard job on bigdisk for a same-repository head passes",
     {"code-review.yml": wf(CR_ON, job(SAME, name="scope"))}, 0, ["OK:"])
case("K2 code-review.yml: the same pool for a fork's head fails (c)",
     {"code-review.yml": wf(CR_ON, job(ANYHEAD, name="scope"))}, 1,
     ["code-review.yml:scope: (c)", "fork, pull_request_target: a fork never reaches a self-hosted runner"],
     ["pull_request_target (refs/heads/main): self-hosted"])
case("K3 signatures.yml: the same-repository exemption is code-review.yml's alone",
     {"signatures.yml": wf(CR_ON, job(SAME, name="signatures"))}, 1, ["signatures.yml:signatures: (c)"])
case("K4 code-review.yml: another self-hosted label for a same-repository head fails (c)",
     {"code-review.yml": wf(CR_ON, job(SAME.replace('"bigdisk"]', '"bigdisk-docker"]'), name="scope"))}, 1,
     ["code-review.yml:scope: (c)"])

# L. preflight takes the self-hosted pool only for a push to main
# and a same-repository pull request; with the variable unset every other event is hosted.
def preflight_labels(event, ref, head):
    ns = {}
    exec(SCRIPT.split("\nfiles = sorted(", 1)[0], ns)
    return ns["runs_on_labels"](PRE, event, ref, head, "unset")


for label, event, ref, head, want in (
        ("a push to main", "push", "refs/heads/main", "", ["self-hosted", "linux", "x64", "bigdisk"]),
        ("a same-repository pull request", "pull_request", "refs/pull/1/merge", "o/r", ["self-hosted", "linux", "x64", "bigdisk"]),
        ("a schedule", "schedule", "refs/heads/main", "", ["ubuntu-latest"]),
        ("a workflow_dispatch on main", "workflow_dispatch", "refs/heads/main", "", ["ubuntu-latest"]),
        ("a push of another branch", "push", "refs/heads/feature", "", ["ubuntu-latest"])):
    got = preflight_labels(event, ref, head)
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}: L1 preflight, CI_PREFLIGHT_RUNS_ON unset, {label}: {want}")
    if not ok:
        FAILS += 1
        print(f"  got {got}")

print(f"{'ALL PASS' if not FAILS else f'{FAILS} FAILED'}")
sys.exit(1 if FAILS else 0)
