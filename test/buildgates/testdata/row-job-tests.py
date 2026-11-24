#!/usr/bin/env python3
"""ci.yml v1.55: the Row job (one SPEC row per pull request).

The Tenant Agent's 12:47Z order (2026-11-22, ruling 1136): a pull request
from a row/<n>-* branch passes when issue #n is named on the BASE commit in a
docs/SPEC.md table row (SPEC only, ruling 1143 (3)); a pull request with an exempt label
(ci, main-fix, std) passes; anything else is red. The script is read out of
the "Row check" step's heredoc and run against a temporary git repository
whose base commit holds the rows and whose head commit adds one more:

  R0  the job's shape: pull_request only, hosted, the base checked out, in
      CI Gate's needs with a `pull` check line.
  R1  the row in a docs/SPEC.md table: green.
  R2  the row in ROADMAP.md only: red (ruling 1143 (3): SPEC only).
  R3  the row absent: red, and the ::error names the rule.
  R4  an exempt label on a non-row branch: green.
  R5  a non-row branch with no label: red, and the ::error names the rule.
  R6  the row only on the head commit: red (the base is read, not the head).
  R7  #n in SPEC prose, not a table row: red.
  R8  #12 does not match #123.
  R9  owner/repo#n does not count.
  R10 a merge-train/ branch: green.
  R11 (LOCAL) the labels API unreachable: a ::warning, the event's labels
      discarded, and a non-row branch is red (fail closed).
  R12 no base commit: red.
  R13 (LOCAL) the event's labels hold an exempt label the live labels API no
      longer returns: red (the live set replaces the event set).
  R14 (LOCAL) the event's labels hold an exempt label, the live labels lookup
      fails, and the row branch names no SPEC row: red (fail closed).

Run: python3 -I test/buildgates/testdata/row-job-tests.py [ci.yml]
"""
import http.server
import json
import os
import re
import subprocess
import sys
import tempfile
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
YML = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "..", "..", ".github", "workflows", "ci.yml")
FAILS = []


def check(cond, msg):
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond:
        FAILS.append(msg)


def job_text(text):
    i = text.find("\n  row:\n")
    if i < 0:
        sys.exit("FAIL: no row job in " + YML)
    m = re.compile(r"\n  [a-z][\w-]*:\n").search(text, i + 1)
    return text[i:m.start() if m else len(text)]


def script(job):
    step = job.find("- name: Row check")
    if step < 0:
        sys.exit("FAIL: no Row check step")
    i = job.find("python3 -I - <<'PY'\n", step)
    j = job.find("\n          PY\n", i)
    if i < 0 or j < 0:
        sys.exit("FAIL: no heredoc in the Row check step")
    body = job[job.index("\n", i) + 1:j + 1]
    return "\n".join(l[10:] for l in body.splitlines()) + "\n"


def shape(text, job):
    check("if: ${{ github.event_name == 'pull_request' && !github.event.pull_request.draft }}" in job,
          "R0 the job runs on a ready pull request only")
    check("fromJSON(needs.preflight.outputs.runner || '\"ubuntu-latest\"')" in job and "needs: [preflight]" in job,
          "R0 the job runs on CI Gate's selected runner (hosted for a fork's head)")
    check("ref: ${{ github.event.pull_request.base.sha }}" in job and "persist-credentials: false" in job,
          "R0 the base is checked out, with no persisted credentials")
    check("ROW_EXEMPT_LABELS: 'ci main-fix std'" in job, "R0 the exempt labels are an env value")
    check(re.search(r"permissions:\n\s+contents: read\n\s+pull-requests: read\n", job) is not None,
          "R0 the job token is read-only")
    gate = text[text.find("\n  ci-gate:\n"):]
    needs = re.search(r"needs: \[([^\]]*)\]", gate)
    check(needs is not None and "row" in [s.strip() for s in needs[1].split(",")], "R0 row is in CI Gate's needs")
    check(re.search(r'check row +"\$\{\{ needs\.row\.result \}\}" pull\n', gate) is not None,
          "R0 CI Gate checks row as pull")
    check('[ "$3" = pull ] && [ "${PULL}" != true ]' in gate and "PULL: ${{ github.event_name == 'pull_request' }}" in gate,
          "R0 CI Gate skips a pull job off pull_request only")


def git(repo, *args):
    subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True,
                   env={**ENV, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
                        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"})


ENV = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent",
       "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}


def repo(tmp):
    r = os.path.join(tmp, "r")
    os.makedirs(os.path.join(r, "docs"))
    git(r, "init", "-q")
    with open(os.path.join(r, "docs", "SPEC.md"), "w") as f:
        f.write("# SPEC\n\nProse names #77 but is not a row.\n\n"
                "| Row | Issue | State |\n|---|---|---|\n"
                "| Store | #41 | done |\n| Seal | #123 | open |\n| Ext | other/repo#55 | open |\n")
    with open(os.path.join(r, "ROADMAP.md"), "w") as f:
        f.write("# ROADMAP\n\n## v1.2\n\n- Barrier rework (#88)\n")
    git(r, "add", "-A")
    git(r, "commit", "-qm", "base")
    base = subprocess.run(["git", "-C", r, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    with open(os.path.join(r, "docs", "SPEC.md"), "a") as f:
        f.write("| New | #99 | open |\n")
    git(r, "commit", "-qam", "head adds row 99")
    return r, base


def run(body, cwd, head, base, labels="[]", url=""):
    env = {**ENV, "HEAD_REF": head, "BASE_SHA": base, "EVENT_LABELS": labels,
           "LABELS_URL": url,
           "ROW_EXEMPT_LABELS": "ci main-fix std", "ROW_EXEMPT_BRANCHES": "merge-train/"}
    if url:
        env["JOB_" + "TOKEN"] = "placeholder"  # a dummy: only a local test server answers
    r = subprocess.run(["python3", "-I", "-"], input=body, cwd=cwd, env=env,
                       capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout + r.stderr


class Labels(http.server.BaseHTTPRequestHandler):
    """LOCAL (R13, R14): a labels API on localhost that answers STATUS with LIVE."""
    LIVE = []
    STATUS = 200

    def do_GET(self):
        data = json.dumps([{"name": n} for n in self.LIVE]).encode()
        self.send_response(self.STATUS)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def live_labels(names, status=200):
    Labels.LIVE = list(names)
    Labels.STATUS = status
    srv = http.server.HTTPServer(("127.0.0.1", 0), Labels)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}/labels"


def main():
    text = open(YML).read()
    job = job_text(text)
    shape(text, job)
    body = script(job)
    with tempfile.TemporaryDirectory() as tmp:
        r, base = repo(tmp)
        rc, out = run(body, r, "row/41-store", base)
        check(rc == 0 and "docs/SPEC.md" in out, f"R1 row #41 in a SPEC table: green (rc {rc})")
        rc, out = run(body, r, "row/88-barrier", base)
        check(rc == 1 and "::error::Row: issue #88" in out and "ROADMAP.md does not count" in out, f"R2 row #88 in ROADMAP only: red, SPEC only (rc {rc})")
        rc, out = run(body, r, "row/5-missing", base)
        check(rc == 1 and "::error::Row: issue #5" in out and "one SPEC row" in out, f"R3 row #5 absent: red, naming the rule (rc {rc})")
        rc, out = run(body, r, "fix/lint", base, labels='["bug", "main-fix"]')
        check(rc == 0 and "exempt label 'main-fix'" in out, f"R4 the main-fix label: green (rc {rc})")
        rc, out = run(body, r, "fix/lint", base, labels='["bug"]')
        check(rc == 1 and "::error::Row: head branch 'fix/lint' is not row/<n>-*" in out and "one SPEC row" in out,
              f"R5 a non-row branch, no exempt label: red, naming the rule (rc {rc})")
        rc, out = run(body, r, "row/99-new", base)
        check(rc == 1 and "::error::Row: issue #99" in out, f"R6 row #99 only on the head: red, the base is read (rc {rc})")
        rc, out = run(body, r, "row/77-prose", base)
        check(rc == 1, f"R7 #77 in SPEC prose only: red (rc {rc})")
        rc, out = run(body, r, "row/12-short", base)
        check(rc == 1, f"R8 #12 does not match #123 (rc {rc})")
        rc, out = run(body, r, "row/55-other", base)
        check(rc == 1, f"R9 other/repo#55 does not count (rc {rc})")
        rc, out = run(body, r, "merge-train/2026-11-22", base)
        check(rc == 0 and "exempt branch prefix" in out, f"R10 a merge-train/ branch: green (rc {rc})")
        rc, out = run(body, r, "chore/x", base, labels='["std"]', url="http://localhost:9/labels")
        check(rc == 1 and "::warning::Row: the live labels could not be read" in out and "exempt label" not in out
              and "::error::Row: head branch 'chore/x' is not row/<n>-*" in out,
              f"R11 labels API unreachable: warning, event labels discarded, red (rc {rc})")
        rc, out = run(body, r, "row/41-store", "")
        check(rc == 1 and "::error::Row: no base commit" in out, f"R12 no base commit: red (rc {rc})")
        srv, url = live_labels(["bug"])
        try:
            rc, out = run(body, r, "fix/lint", base, labels='["ci"]', url=url)
        finally:
            srv.shutdown()
            srv.server_close()
        check(rc == 1 and "::warning::" not in out and "::error::Row: head branch 'fix/lint' is not row/<n>-*" in out,
              f"R13 an exempt label removed since the event: red, the live labels replace the event's (rc {rc})")
        srv, url = live_labels([], status=500)
        try:
            rc, out = run(body, r, "row/5-missing", base, labels='["ci"]', url=url)
        finally:
            srv.shutdown()
            srv.server_close()
        check(rc == 1 and "::warning::Row: the live labels could not be read" in out and "exempt label" not in out
              and "::error::Row: issue #5" in out,
              f"R14 the live labels lookup fails: the event's exempt label is discarded, no SPEC row: red (rc {rc})")
    print(f"{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILED'}")
    sys.exit(1 if FAILS else 0)


main()
