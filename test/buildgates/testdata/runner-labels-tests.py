#!/usr/bin/env python3
"""runner-labels-tests.py -- runs ci.yml's `Runner labels (ruling 633)` step
(ci-template v1.38) as the workflow runs it and asserts what it passes and
fails.

The code under test is read out of ci.yml, not copied: the python heredoc of
the step named `Runner labels (ruling 633)`. The step name and the heredoc
must each be found exactly once, or the test stops with FATAL (exit 9) and
measures nothing. Each case writes a repository's .github/workflows/ into a
temporary directory and runs the script there with `python3 -I -`, as the
step does; nothing reaches the network.

  A. Passes: the template directory's own workflows (ci.yml's publish-images
     included); a tag-push release.yml on the release label; a dispatch on
     main; the push-or-dispatch guard folded over lines (`if: >-`); the
     runs-on mapping form; a guard written in another case; a label that only
     starts with `release`; a PR workflow with no release job.
  B. Fails (a), a pull request can run a release job: no `if:`; an
     expression runs-on that picks `release` on main and another label on a
     PR (go-xkms ci-images); a block-list runs-on; pull_request_target with a
     ref-only guard; triggers in a flow mapping.
  C. Fails (b), the `if:` does not restrict the job to the allow-list: a ref
     check with no event check (schedule runs on main); an event check with no
     ref check; a tag-prefix check alone (a dispatch on a tag, a release
     event); RELEASE_RUNNER alone; success(); text around the expression; an
     `if:` that cannot be lexed; a .yaml file is read too. A (b) finding in a
     workflow with no pull request trigger is not also an (a) finding.
  D. ci-template v1.43 (Tenant Agent ruling 833, owner: GitHub-hosted is
     the default): on github.com every job that does not name `release`
     runs on a standard GitHub-hosted image (ubuntu-latest, ubuntu-24.04,
     ubuntu-22.04, their -arm forms, ubuntu-slim) on every event (c), unless
     its runs-on line carries `# self-hosted: <reason>`; a larger runner
     (cores, large, xlarge, gpu) is refused even with a waiver (d), since it
     bills on a public repository. The routing expression the P1 repositories
     carry (self-hosted for a push to main and a same-repository PR) fails
     (c), the template's server-conditional form passes. ci-template v1.46
     (Tenant Agent ruling 999, owner: self-hosted only for hardware-specific
     or oversize jobs): the reason names its category, `# self-hosted:
     hardware: <reason>` or `# self-hosted: oversize: <reason>`
     (`credential:` for the one Codex review job); a waiver with no category,
     or an unknown one, is (e). ci-template v1.47 (Tenant Agent ruling
     1007; go-qrdb #1642's Codex review): label and category are checked
     together. The review-only labels codex and qwen run only in
     code-review.yml, codex with `credential:` and qwen with `hardware:`;
     `credential:` is codex's alone; any other pairing is (f), whatever the
     waiver says.
  E. ci-template v1.48 (Tenant Agent ruling 1024 item 1): in ci.yml a
     runs-on that reads the preflight's runner selection,
     fromJSON(needs.preflight.outputs.runner) or runner_docker, passes when
     its default gives a standard image and its overflow gives exactly
     [self-hosted, linux, x64, bigdisk] or [..., bigdisk-docker]. The same
     text outside ci.yml, the output without fromJSON, the list written out,
     a self-hosted value on some event, an overflow to another label, and
     another job's output all stay (c).
  F. ci-template v1.49 (Tenant Agent ruling 1047 (3), (2)): ci.yml's
     preflight job runs on the Dells' bigdisk pool on github.com unless the
     repository variable CI_PREFLIGHT_RUNS_ON overrides it. Its runs-on
     passes unset (exactly [self-hosted, linux, x64, bigdisk]) and set to the
     fallback `"ubuntu-latest"` (a standard image). The template's own
     ci.yml preflight passes (A1 too). The same text in another job, outside
     ci.yml, a default to any other label, a value that stays self-hosted
     with the variable set, and the variable read without fromJSON all stay
     (c).
  G. ci-template v1.51 (ruling 633 hardening): every runs-on is also read
     under pull_request_target, same-repository and fork, the event
     code-review.yml and signatures.yml run on. The two templates pass; a
     label picked only under pull_request_target, or written out, is (c) on
     that event (every event is read in every workflow, as before); in a
     workflow with that trigger a fork's head gets neither
     the selection's overflow nor the unset-variable exemption, and the
     finding says a fork never reaches a self-hosted runner. Under
     pull_request alone the same text keeps its v1.48/v1.49 meaning for a
     same-repository head.
  H. ci-template v1.52 (Tenant Agent order, defect fix): a fork's pull
     request under pull_request reached a self-hosted runner. ci.yml's
     preflight runs-on now carries a fork clause, and the lint gives a fork's
     head under pull_request, as under pull_request_target, no
     unset-variable exemption; the selection outputs read for it are the
     hosted default the step emits (runner-select-tests.py part E). The
     v1.49 preflight form is (c) for a fork's head only; every job of the
     template ci.yml, evaluated with the lint's own reader, runs on
     ubuntu-latest for a fork's pull request on github.com, while a
     same-repository pull request keeps the Dells' default and overflow.

Run: python3 -I software/templates/github-actions/testdata/runner-labels-tests.py
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
    if text.count("- name: Runner labels (ruling 633)\n") != 1:
        fatal("the step `Runner labels (ruling 633)` is not in ci.yml exactly once")
    found = re.findall(
        r"- name: Runner labels \(ruling 633\)\n        run: \|\n          python3 -I - <<'PY'\n(.*?)\n          PY\n",
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
case("A1 the template directory's own workflows",
     {p.name: p.read_text() for p in WORKFLOWS.glob("*.yml")}, 0, ["OK:"])
case("A2 release.yml: tag push on the release label",
     {"release.yml": wf("on:\n  push:\n    tags: ['v*']\n",
                        job(REL, "startsWith(github.ref, 'refs/tags/') && github.event_name == 'push'"))}, 0)
case("A3 workflow_dispatch on main",
     {"r.yml": wf("on: [workflow_dispatch]\n",
                  job(REL, "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main'"))}, 0)
case("A4 push-or-dispatch guard folded over lines, beside a PR trigger",
     {"ci.yml": wf(PR_ON, job(REL, ">-\n      ${{ (github.event_name == 'push' &&\n"
                                   "      (github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/')))\n"
                                   "      || (github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main') }}"),  # LOCAL: K2
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
     ["ci.yml:pub: (b)", "ci.yml:pub: (a)"])  # LOCAL: K9: a merged pull request runs on refs/heads/main

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

# D. hosted first (v1.43)
ROUTE = ("${{ ((github.event_name == 'push' && github.ref == 'refs/heads/main') || "
         "(github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository)) "
         "&& fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk-docker\"]') || 'ubuntu-latest' }}")
case("D1 ubuntu-24.04, ubuntu-latest-arm-free forms and ubuntu-slim pass",
     {"ci.yml": wf(PR_ON, job("ubuntu-24.04"), job("ubuntu-24.04-arm", name="arm"), job("ubuntu-slim", name="slim"))}, 0)
case("D2 the template's server-conditional form passes",
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

# E. the runner selection's overflow (v1.48, ruling 1024)
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
     {"ci.yml": wf(PR_ON, job("${{ needs.preflight.outputs.runner_docker == '\"ubuntu-latest\"' && 'ubuntu-latest' || 'dells' }}",
                              name="other"))}, 1, ["ci.yml:other: (c)", "runner selection overflow"])
case("E7 another job's output is not the selection: (c)",
     {"ci.yml": wf(PR_ON, job("${{ fromJSON(needs.route.outputs.runner) }}", name="dyn"))}, 1, ["ci.yml:dyn: (c)"])

# F. preflight on the Dells by default, the variable its fallback (v1.49, ruling 1047)
PRE_V149 = ("${{ github.server_url == 'https://github.com' && fromJSON(vars.CI_PREFLIGHT_RUNS_ON || "
            "'[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || 'ubuntu-latest' }}")
# v1.52: the fork clause before the variable.
PRE = PRE_V149.replace("&& fromJSON(vars.", "&& (!startsWith(github.event_name, 'pull_request') || "
                       "github.event.pull_request.head.repo.full_name == github.repository) && fromJSON(vars.")
PRE_TEXT = (WORKFLOWS / "ci.yml").read_text()
if PRE_TEXT.count(f"    runs-on: {PRE}   # v1.52\n") != 1:
    fatal("ci.yml's preflight runs-on is not the form part F tests, exactly once")
case("F1 ci.yml: preflight's runs-on passes, unset and set to the fallback",
     {"ci.yml": wf(PR_ON, job(PRE, name="preflight"))}, 0, ["OK:"])
case("F2 the same runs-on on another ci.yml job fails (c): the variable is preflight's alone",
     {"ci.yml": wf(PR_ON, job(PRE, name="lint"))}, 1, ["ci.yml:lint: (c)", "an unknown label"],  # LOCAL: the fallback is not
     ["CI_PREFLIGHT_RUNS_ON"])  # the guaranteed value when the variable is unknown
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
case("F8 the off-github.com label is not checked: the forge keeps ubuntu-latest",
     {"ci.yml": wf(PR_ON, job(PRE.replace("|| 'ubuntu-latest' }}",  # v1.52: that tail is also a fork's
                                          "|| github.server_url != 'https://github.com' && 'bigdisk' || 'ubuntu-latest' }}"),
                              name="preflight"))}, 0, ["OK:"])

# G. pull_request_target, same-repository and fork (v1.51, ruling 633 hardening)
PRT_ON = ("on:\n  pull_request_target:\n    branches: [main]\n    types: [opened, synchronize]\n"
          "  push:\n    branches: [main]\n")
PRT_ONLY = "${{ github.event_name == 'pull_request_target' && 'bigdisk' || 'ubuntu-latest' }}"
for name in ("code-review.yml", "signatures.yml"):
    if not (WORKFLOWS / name).exists():  # LOCAL: signatures is a ci.yml job here
        continue
    case(f"G1 {name}: the template passes alone",
         {name: (WORKFLOWS / name).read_text()}, 0, ["OK:"])
case("G2 a label picked only under pull_request_target fails (c) on that event, both heads",
     {"signatures.yml": wf(PRT_ON, job(PRT_ONLY, name="signatures"))}, 1,
     ["signatures.yml:signatures: (c)", "pull_request_target (refs/heads/main): bigdisk",
      "pull_request_target (refs/heads/main, fork, pull_request_target: a fork never reaches a self-hosted runner): bigdisk"])
case("G3 the same text fails without the trigger too, as every event is read in every workflow; no fork note",
     {"signatures.yml": wf(PR_ON, job(PRT_ONLY, name="signatures"))}, 1,
     ["signatures.yml:signatures: (c)", "pull_request_target (refs/heads/main, fork): bigdisk"])
case("G4 a hard-coded self-hosted label under pull_request_target fails (c) on both heads",
     {"signatures.yml": wf(PRT_ON, job("[self-hosted, linux, x64, bigdisk]", name="signatures"))}, 1,
     ["signatures.yml:signatures: (c)", "pull_request_target (refs/heads/main):",
      "pull_request_target (refs/heads/main, fork"])
case("G5 ci.yml with pull_request_target: the unguarded selection gets no overflow for a fork's head",
     {"ci.yml": wf(PRT_ON, job(SEL_PLAIN, name="lint"))}, 1,
     ["ci.yml:lint: (c)", "pull_request_target (refs/heads/main, fork, runner selection overflow, "
      "pull_request_target: a fork never reaches a self-hosted runner)"])
case("G6 ci.yml under pull_request: the same selection keeps its v1.48 overflow (same-repository head)",
     {"ci.yml": wf(PR_ON, job(SEL_PLAIN, name="lint"))}, 0, ["OK:"])
case("G7 ci.yml with pull_request_target: preflight's unset default is no exemption for a fork's head",
     {"ci.yml": wf(PRT_ON, job(PRE_V149, name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", "fork, CI_PREFLIGHT_RUNS_ON unset, pull_request_target: a fork never reaches"])
case("G8 a waived self-hosted job under pull_request_target is still the waiver's",
     {"signatures.yml": wf(PRT_ON, job("[self-hosted, linux, x64, bigdisk]  # self-hosted: oversize: test",
                                       name="signatures"))}, 0, ["OK:"])

# H. a fork's pull request under pull_request (v1.52, Tenant Agent order)
FORK_NOTE = "pull_request (refs/pull/1/merge, fork, CI_PREFLIGHT_RUNS_ON unset, pull_request: a fork never reaches a self-hosted runner)"
case("H1 ci.yml: the v1.49 preflight runs-on fails (c) for a fork's head under pull_request",
     {"ci.yml": wf(PR_ON, job(PRE_V149, name="preflight"))}, 1,
     ["ci.yml:preflight: (c)", FORK_NOTE + ": self-hosted, linux, x64, bigdisk"],
     ["pull_request (refs/pull/1/merge, CI_PREFLIGHT_RUNS_ON unset", "push (refs/heads/main"])
case("H2 ci.yml: the v1.52 preflight runs-on passes under pull_request and pull_request_target",
     {"ci.yml": wf(PR_ON.replace("on:\n", "on:\n  pull_request_target:\n    branches: [main]\n"),
                   job(PRE, name="preflight"))}, 0, ["OK:"])
CI_TEXT = (WORKFLOWS / "ci.yml").read_text()
case("H3 the template ci.yml with preflight's v1.49 runs-on fails (c), for the fork alone",
     {"ci.yml": CI_TEXT.replace(f"    runs-on: {PRE}   # v1.52\n", f"    runs-on: {PRE_V149}   # v1.49\n")}, 1,
     ["ci.yml:preflight: (c)", FORK_NOTE], ["ci.yml:lint:", "ci.yml:image:", "ci.yml:ci-gate:"])
case("H4 a selection job that routes a fork's head to the Dells itself fails (c) under pull_request",
     {"ci.yml": wf(PR_ON, job("${{ github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name != github.repository && "
                              "fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || "
                              "fromJSON(needs.preflight.outputs.runner) }}", name="lint"))}, 1,
     ["ci.yml:lint: (c)", "pull_request (refs/pull/1/merge, fork, runner selection overflow, pull_request: a fork never reaches"],
     ["push (refs/heads/main", "pull_request (refs/pull/1/merge):"])


def every_job(head):
    # Each job's runs-on in the template ci.yml, read by the lint's own reader on
    # github.com under pull_request, in every state of its inputs it is checked in.
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


# LOCAL: the jobs the template gives the selection (preflight its own default).
# This repository's other ci.yml jobs stay on ubuntu-latest on every event.
SELECTION_JOBS = {"preflight", "lint", "image", "lint-gates", "integration", "soak", "ci-gate"}


def selection_case(label, head, want):
    global FAILS
    got = every_job(head)
    bad = {n: v for n, v in got.items() if any(want(n, sel, l) is False for sel, l in v.items())}
    ok = len(got) >= 6 and SELECTION_JOBS <= set(got) and not bad
    print(f"{'PASS' if ok else 'FAIL'}: {label}")
    if not ok:
        FAILS += 1
        print(f"  {len(got)} job(s) read; off: {bad or got}")


selection_case("H5 the template ci.yml: every job runs on ubuntu-latest for a fork's pull request on github.com",
               "fork/r", lambda n, sel, l: l == ["ubuntu-latest"])
selection_case("H6 the template ci.yml: a same-repository pull request keeps the Dells' default and overflow",
               "o/r", lambda n, sel, l: l == (["ubuntu-latest"] if sel in ("set", "default") or n not in SELECTION_JOBS else
                                             ["self-hosted", "linux", "x64", "bigdisk"] if n in ("preflight", "ci-gate")
                                             else ["self-hosted", "linux", "x64", "bigdisk-docker"]))

# I. code-review.yml's guard jobs on the Dells for a same-repository head (v1.53, Tenant Agent on #821)
CR_ON = "on:\n  pull_request_target:\n    branches: [main]\n    types: [opened, synchronize]\n"
SAME = ("${{ github.server_url == 'https://github.com' && (github.event.pull_request.head.repo.full_name == github.repository "
        "&& fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || 'ubuntu-latest') || 'bigdisk' }}")
ANYHEAD = ("${{ github.server_url == 'https://github.com' && (github.event_name == 'pull_request_target' "
           "&& fromJSON('[\"self-hosted\",\"linux\",\"x64\",\"bigdisk\"]') || 'ubuntu-latest') || 'bigdisk' }}")
case("I1 code-review.yml: a guard job on bigdisk for a same-repository head passes",
     {"code-review.yml": wf(CR_ON, job(SAME, name="scope"))}, 0, ["OK:"])
case("I2 code-review.yml: the same pool for a fork's head fails (c)",
     {"code-review.yml": wf(CR_ON, job(ANYHEAD, name="scope"))}, 1,
     ["code-review.yml:scope: (c)", "fork, pull_request_target: a fork never reaches a self-hosted runner"],
     ["pull_request_target (refs/heads/main): self-hosted"])
case("I3 signatures.yml: the same-repository exemption is code-review.yml's alone",
     {"signatures.yml": wf(CR_ON, job(SAME, name="signatures"))}, 1, ["signatures.yml:signatures: (c)"])
case("I4 code-review.yml: another self-hosted label for a same-repository head fails (c)",
     {"code-review.yml": wf(CR_ON, job(SAME.replace('"bigdisk"]', '"bigdisk-docker"]'), name="scope"))}, 1,
     ["code-review.yml:scope: (c)"])


# J. an unknown left operand keeps the result unknown (LOCAL: a fallback is not the guaranteed value)
case("J1 a release guard whose fallback compares false is unknown, not false: (b)",
     {"r.yml": wf("on: push\n", job(REL, "${{ (vars.MODE || 'disabled') != 'disabled' }}"))}, 1, ["r.yml:pub: (b)"])
case("J2 a variable with a hosted fallback is not hosted routing: (c)",
     {"ci.yml": wf(PR_ON, job("${{ vars.RUNNER || 'ubuntu-latest' }}", name="lint"))}, 1, ["ci.yml:lint: (c)"])
case("J3 an unknown left operand of && keeps a false right operand's falsiness",
     {"r.yml": wf("on: push\n", job(REL, "${{ vars.MODE && false }}"))}, 0, ["OK:"])


# K. the sampled contexts are not exhaustive (LOCAL): a guard's own literals are tried as refs and events
case("K1 a push guard on another branch fails (b)",
     {"r.yml": wf("on: push\n", job(REL, "${{ github.event_name == 'push' && github.ref == 'refs/heads/dev' }}"))}, 1,
     ["r.yml:pub: (b)", "push (refs/heads/dev)"])
case("K2 workflow_dispatch checked by ref_name alone fails (b): a tag can carry the branch's name",
     {"r.yml": wf("on: [workflow_dispatch]\n",
                  job(REL, "${{ github.event_name == 'workflow_dispatch' && github.ref_name == 'main' }}"))}, 1,
     ["r.yml:pub: (b)", "workflow_dispatch (refs/tags/main)"])
case("K3 an event the samples lack, named by the guard, fails (b)",
     {"r.yml": wf("on: [deployment]\n", job(REL, "${{ github.event_name == 'deployment' }}"))}, 1,
     ["r.yml:pub: (b)", "deployment ("])

case("K4 a prefix and a suffix together still leave a non-main branch: (b)",
     {"r.yml": wf("on: push\n", job(REL, "${{ github.event_name == 'push' && startsWith(github.ref, 'refs/heads/hotfix/') "
                                         "&& endsWith(github.ref, '/deploy') }}"))}, 1, ["r.yml:pub: (b)", "push (any other ref)"])
case("K5 a ref_type check alone does not prove a tag push for a dispatch: (b)",
     {"r.yml": wf("on: [push, workflow_dispatch]\n", job(REL, "${{ github.ref_type == 'tag' }}"))}, 1,
     ["r.yml:pub: (b)", "workflow_dispatch ("])

case("K6 a guard naming another pull request's ref fails (a)",
     {"ci.yml": wf(PR_ON, job(REL, "${{ github.event_name == 'pull_request' && github.ref == 'refs/pull/2/merge' }}"))}, 1,
     ["ci.yml:pub: (a)"])
case("K7 a guard naming a pull request's head branch fails (a)",
     {"ci.yml": wf(PR_ON, job(REL, "${{ github.event_name == 'pull_request' && github.head_ref == 'hotfix' }}"))}, 1,
     ["ci.yml:pub: (a)"])
case("K8 a guard naming a pull request's base branch fails (a)",
     {"ci.yml": wf(PR_ON, job(REL, "${{ github.event_name == 'pull_request' && github.base_ref == 'release' }}"))}, 1,
     ["ci.yml:pub: (a)"])
case("K9 a closed pull request's guard naming its base branch ref fails (a): a merged pull request runs on that ref",
     {"ci.yml": wf("on:\n  pull_request:\n    types: [closed]\n",
                   job(REL, "${{ github.event_name == 'pull_request' && github.ref == 'refs/heads/main' }}"))}, 1,
     ["ci.yml:pub: (a)"])
case("K10 arrays compare by identity, so two decoded arrays are never equal: the guard is unknown and fails",
     {"ci.yml": wf(PR_ON, job(REL, "${{ fromJSON('[]') != fromJSON('[]') }}"))}, 1,
     ["ci.yml:pub: ("])

print(f"{'ALL PASS' if not FAILS else f'{FAILS} FAILED'}")
sys.exit(1 if FAILS else 0)
