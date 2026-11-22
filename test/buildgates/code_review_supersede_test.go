package buildgates

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The two steps of code-review.yml that mutate workflow runs with
// `actions: write`: the supersede job's cancel of this PR's runs on an older
// head, and the gate's re-run of a newer twin's red gate on the same head.
// Each test extracts the step's shell from the workflow, runs it with a stub
// curl first on PATH that serves canned API answers from local files and logs
// every request, and asserts the exact cancel and re-run requests. Nothing
// leaves the machine: the API base is a .invalid name the stub strips.

const (
	stepAPIBase  = "https://forge.invalid/repos/owner/repo"
	stepPR       = "42"
	stepRunID    = "500"
	stepWorkflow = "77"
)

var (
	stepHead  = strings.Repeat("a", 40) // the PR's live head
	stepOld   = strings.Repeat("b", 40) // an older head of the same PR
	stepBase  = strings.Repeat("c", 40) // a pull_request_target run's head_sha
	stepOther = strings.Repeat("d", 40)
)

// stepStubCurl answers each request from $STUB_ROUTES, lines of
// "METHOD<TAB>path<TAB>code<TAB>body file"; code "unreachable" makes curl
// fail as a dropped connection does. A request with no route is logged as
// UNEXPECTED and answered 404.
const stepStubCurl = `#!/usr/bin/env bash
set -u
method=GET out=/dev/null url=
while [ $# -gt 0 ]; do
  case "$1" in
    -X) method=$2; shift 2 ;;
    -o) out=$2; shift 2 ;;
    -w|-H|--proto|--max-time|--retry) shift 2 ;;
    -*) shift ;;
    *) url=$1; shift ;;
  esac
done
path=${url#"$STUB_BASE"}
echo "$method $path" >>"$STUB_LOG"
while IFS=$'\t' read -r m p code body; do
  if [ "$m" = "$method" ] && [ "$p" = "$path" ]; then
    [ "$code" != unreachable ] || exit 7
    cp "$body" "$out"
    printf '%s' "$code"
    exit 0
  fi
done <"$STUB_ROUTES"
echo "UNEXPECTED $method $path" >>"$STUB_LOG"
printf '{}' >"$out"
printf '404'
`

type stepAnswer struct{ code, body string }

func ok(body string) stepAnswer { return stepAnswer{"200", body} }

// extractRunStep returns the `run: |` block of the one step named step.
func extractRunStep(t *testing.T, step string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "code-review.yml"))
	if err != nil {
		t.Fatalf("read code-review.yml: %v", err)
	}
	lines := strings.Split(string(b), "\n")
	var found []string
	for i, l := range lines {
		if strings.TrimSpace(l) != "- name: "+step {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			ts := strings.TrimSpace(lines[j])
			if strings.HasPrefix(ts, "- name:") {
				break
			}
			if ts != "run: |" {
				continue
			}
			var out []string
			indent := ""
			for _, f := range lines[j+1:] {
				if strings.TrimSpace(f) == "" {
					out = append(out, "")
					continue
				}
				ind := f[:len(f)-len(strings.TrimLeft(f, " "))]
				if indent == "" {
					indent = ind
				}
				if len(ind) < len(indent) {
					break
				}
				out = append(out, strings.TrimPrefix(f, indent))
			}
			found = append(found, strings.Join(out, "\n")+"\n")
			break
		}
	}
	if len(found) != 1 {
		t.Fatalf("code-review.yml: %d steps named %q with a run: | block, want 1", len(found), step)
	}
	return found[0]
}

// runAPIStep runs script under bash with the stub curl and returns its exit
// code, its output and the logged requests.
func runAPIStep(t *testing.T, script string, env []string, routes map[string]stepAnswer) (int, string, []string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(stepStubCurl), 0o700); err != nil { //nolint:gosec // the stub must be executable
		t.Fatal(err)
	}
	keys := make([]string, 0, len(routes))
	for k := range routes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var table strings.Builder
	for i, k := range keys {
		method, path, okSplit := strings.Cut(k, " ")
		if !okSplit {
			t.Fatalf("route %q: want \"METHOD path\"", k)
		}
		f := filepath.Join(dir, fmt.Sprintf("answer-%d.json", i))
		if err := os.WriteFile(f, []byte(routes[k].body), 0o600); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&table, "%s\t%s\t%s\t%s\n", method, path, routes[k].code, f)
	}
	routesFile := filepath.Join(dir, "routes.tsv")
	logFile := filepath.Join(dir, "requests.log")
	if err := os.WriteFile(routesFile, []byte(table.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "step.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(dir, "work")
	if err := os.Mkdir(work, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), bash, path)
	cmd.Dir = work
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"STUB_BASE="+stepAPIBase,
		"STUB_ROUTES="+routesFile,
		"STUB_LOG="+logFile,
		"GH_API_TOKEN=test-token",
		"REPO_API="+stepAPIBase,
		"PR_NUM="+stepPR,
		"GITHUB_RUN_ID="+stepRunID,
	)
	cmd.Env = append(cmd.Env, env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	code := 0
	if err := cmd.Run(); err != nil {
		ee, isExit := err.(*exec.ExitError)
		if !isExit {
			t.Fatalf("run bash: %v", err)
		}
		code = ee.ExitCode()
	}
	logged, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	var reqs []string
	for l := range strings.SplitSeq(strings.TrimSpace(string(logged)), "\n") {
		if l != "" {
			reqs = append(reqs, l)
		}
	}
	return code, out.String(), reqs
}

type stepCase struct {
	name   string
	routes map[string]stepAnswer
	// posts is the exact list of mutating requests, in order.
	posts []string
	want  []string
	// warn: the step stops on a warning (exit 0, the best-effort contract).
	warn bool
}

func checkStepCase(t *testing.T, script string, env []string, c stepCase) {
	t.Helper()
	code, out, reqs := runAPIStep(t, script, env, c.routes)
	if code != 0 {
		t.Errorf("exit %d, want 0 (the step is best effort)\n%s", code, out)
	}
	if len(reqs) == 0 {
		t.Fatalf("no request reached the stub: curl was not the stub\n%s", out)
	}
	var posts []string
	for _, r := range reqs {
		if strings.HasPrefix(r, "UNEXPECTED ") {
			t.Errorf("request with no canned answer: %s", r)
		}
		if !strings.HasPrefix(r, "GET ") && !strings.HasPrefix(r, "UNEXPECTED ") {
			posts = append(posts, r)
		}
	}
	if !slices.Equal(posts, c.posts) {
		t.Errorf("mutating requests %q, want %q\n%s", posts, c.posts, out)
	}
	for _, w := range c.want {
		if !strings.Contains(out, w) {
			t.Errorf("output lacks %q\n%s", w, out)
		}
	}
	if got := strings.Contains(out, "::warning::"); got != c.warn {
		t.Errorf("warning printed = %v, want %v\n%s", got, c.warn, out)
	}
}

func runsJSON(runs ...string) string {
	return `{"workflow_runs":[` + strings.Join(runs, ",") + `]}`
}

func jobsJSON(jobs ...string) string { return `{"jobs":[` + strings.Join(jobs, ",") + `]}` }

func jobJSON(id int, name, status, conclusion string) string {
	return fmt.Sprintf(`{"id":%d,"name":%q,"status":%q,"conclusion":%q}`, id, name, status, conclusion)
}

func reviewTitle(pr, base, sha string) string {
	return fmt.Sprintf("Code Review: PR #%s into %s at %s", pr, base, sha)
}

// --- supersede: "Cancel this PR's runs on an older head" ---

var supersedeStatuses = []string{"queued", "in_progress", "waiting", "pending", "requested"}

// supersedeList is the first page of a status's run list (v5.13.0 reads
// page after page; a page with fewer than 100 runs is the last).
func supersedeList(st string) string {
	return "GET /actions/workflows/" + stepWorkflow + "/runs?event=pull_request_target&status=" + st + "&per_page=100&page=1"
}

func supersedeRun(id int, pr, titleSHA, headSHA string) string {
	return fmt.Sprintf(`{"id":%d,"head_sha":%q,"display_title":%q}`, id, headSHA, reviewTitle(pr, "main", titleSHA))
}

func supersedeJobs(id int) string {
	return fmt.Sprintf("GET /actions/runs/%d/jobs?per_page=100", id)
}

func supersedeCancel(id int) string { return fmt.Sprintf("POST /actions/runs/%d/cancel", id) }

// supersedeRoutes: the PR is open on stepHead, every status list is empty
// unless lists names it, and every run's jobs are an idle review.
func supersedeRoutes(lists map[string][]string, ids ...int) map[string]stepAnswer {
	r := map[string]stepAnswer{
		"GET /pulls/" + stepPR:           ok(fmt.Sprintf(`{"state":"open","head":{"sha":%q}}`, stepHead)),
		"GET /actions/runs/" + stepRunID: ok(`{"id":500,"workflow_id":` + stepWorkflow + `}`),
	}
	for _, st := range supersedeStatuses {
		r[supersedeList(st)] = ok(runsJSON(lists[st]...))
	}
	for _, id := range ids {
		r[supersedeJobs(id)] = ok(jobsJSON(jobJSON(id*10, "Codex review", "queued", "")))
		r[supersedeCancel(id)] = stepAnswer{"202", "{}"}
	}
	return r
}

func with(r map[string]stepAnswer, k string, a stepAnswer) map[string]stepAnswer {
	r[k] = a
	return r
}

func TestCodeReviewSupersedeCancel(t *testing.T) {
	script := extractRunStep(t, "Cancel this PR's runs on an older head")
	env := []string{"EVENT_SHA=" + stepHead}
	cases := []stepCase{
		{
			name: "permitted: this PR's older-head runs, queued and running with an idle review, once each",
			routes: supersedeRoutes(map[string][]string{
				"queued":      {supersedeRun(400, stepPR, stepOld, stepBase)},
				"in_progress": {supersedeRun(401, stepPR, stepOld, stepBase), supersedeRun(400, stepPR, stepOld, stepBase)},
			}, 400, 401),
			posts: []string{supersedeCancel(400), supersedeCancel(401)},
			want:  []string{"Run 400 is on an older head", "Run 401 is on an older head"},
		},
		{
			name: "same head: a run whose title or head_sha names this head is never cancelled",
			routes: supersedeRoutes(map[string][]string{
				"queued":      {supersedeRun(410, stepPR, stepHead, stepBase)},
				"in_progress": {supersedeRun(411, stepPR, stepOld, stepHead)},
			}, 410, 411),
			want: []string{"No run of PR 42 on an older head"},
		},
		{
			name: "newer run: a run started after this one is never cancelled",
			routes: supersedeRoutes(map[string][]string{
				"queued": {supersedeRun(600, stepPR, stepOld, stepBase)},
			}, 600),
			want: []string{"No run of PR 42 on an older head"},
		},
		{
			name: "active review: a run whose codex or opencode review is in progress is left alone",
			routes: with(with(supersedeRoutes(map[string][]string{
				"in_progress": {supersedeRun(420, stepPR, stepOld, stepBase), supersedeRun(421, stepPR, stepOld, stepBase)},
			}, 420, 421),
				supersedeJobs(420), ok(jobsJSON(jobJSON(1, "Review scope (InfoSec guard)", "completed", "success"),
					jobJSON(2, "Codex review", "in_progress", "")))),
				supersedeJobs(421), ok(jobsJSON(jobJSON(3, "Code Review (opencode)", "in_progress", "")))),
			want: []string{"Run 420: its review is in progress", "Run 421: its review is in progress"},
		},
		{
			name: "other PR: another PR's runs, PR #4 and PR #420 included, are never cancelled",
			routes: supersedeRoutes(map[string][]string{
				"queued": {supersedeRun(430, "4", stepOld, stepBase), supersedeRun(431, "420", stepOld, stepBase),
					fmt.Sprintf(`{"id":432,"head_sha":%q,"display_title":"PR #42 at %s"}`, stepBase, stepOld)},
			}, 430, 431, 432),
			want: []string{"No run of PR 42 on an older head"},
		},
		{
			name: "superseded event: when the live head is another commit this run cancels nothing",
			routes: with(supersedeRoutes(map[string][]string{
				"queued": {supersedeRun(400, stepPR, stepOld, stepBase)},
			}, 400), "GET /pulls/"+stepPR, ok(fmt.Sprintf(`{"state":"open","head":{"sha":%q}}`, stepOther))),
			want: []string{"live head is " + stepOther + ", not " + stepHead + "; nothing to cancel"},
		},
		{
			name: "closed PR: nothing is listed or cancelled",
			routes: with(supersedeRoutes(nil), "GET /pulls/"+stepPR,
				ok(fmt.Sprintf(`{"state":"closed","head":{"sha":%q}}`, stepHead))),
			want: []string{"live head is not open"},
		},
		{
			// Recorded behaviour: the step warns and stops at the refused
			// cancel, so run 401 is left to its own stale guard.
			name: "race: a run that ended between list and cancel is a warning that stops the step",
			routes: with(supersedeRoutes(map[string][]string{
				"queued": {supersedeRun(400, stepPR, stepOld, stepBase), supersedeRun(401, stepPR, stepOld, stepBase)},
			}, 400, 401), supersedeCancel(400), stepAnswer{"409", `{"message":"Cannot cancel a workflow run that is completed."}`}),
			posts: []string{supersedeCancel(400)},
			want:  []string{"POST /actions/runs/400/cancel answered HTTP 409"},
			warn:  true,
		},
		{
			name: "API error: a list answering 500 cancels nothing",
			routes: with(supersedeRoutes(map[string][]string{
				"in_progress": {supersedeRun(401, stepPR, stepOld, stepBase)},
			}, 401), supersedeList("queued"), stepAnswer{"500", `{"message":"boom"}`}),
			want: []string{"answered HTTP 500"},
			warn: true,
		},
		{
			name: "API error: a jobs read that fails cancels nothing",
			routes: with(supersedeRoutes(map[string][]string{
				"queued": {supersedeRun(400, stepPR, stepOld, stepBase)},
			}, 400), supersedeJobs(400), stepAnswer{"unreachable", ""}),
			want: []string{"cannot reach " + stepAPIBase + "/actions/runs/400/jobs"},
			warn: true,
		},
		{
			name: "API error: an unreadable PR cancels nothing",
			routes: with(supersedeRoutes(map[string][]string{
				"queued": {supersedeRun(400, stepPR, stepOld, stepBase)},
			}, 400), "GET /pulls/"+stepPR, stepAnswer{"502", "{}"}),
			want: []string{"answered HTTP 502"},
			warn: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { checkStepCase(t, script, env, c) })
	}
}

// --- gate handoff: "Re-run a newer twin's failed gate on this head" ---

const handoffSuite = 1000

func handoffList() string {
	return "GET /actions/workflows/" + stepWorkflow + "/runs?head_sha=" + stepHead + "&per_page=100"
}

func handoffRun(id, suite int, status, pr, base string) string {
	return fmt.Sprintf(`{"id":%d,"check_suite_id":%d,"status":%q,"head_sha":%q,"display_title":%q}`,
		id, suite, status, stepHead, reviewTitle(pr, base, stepHead))
}

func handoffRerun(job int) string { return fmt.Sprintf("POST /actions/jobs/%d/rerun", job) }

// handoffRoutes lists runs; each run id's jobs are a red gate, job id*10.
func handoffRoutes(runs []string, ids ...int) map[string]stepAnswer {
	r := map[string]stepAnswer{
		"GET /actions/runs/" + stepRunID: ok(fmt.Sprintf(`{"id":500,"workflow_id":%s,"check_suite_id":%d}`, stepWorkflow, handoffSuite)),
		handoffList():                    ok(runsJSON(runs...)),
	}
	for _, id := range ids {
		r[supersedeJobs(id)] = ok(jobsJSON(jobJSON(id*10+1, "Codex review", "completed", "skipped"),
			jobJSON(id*10, "Code Review (codex)", "completed", "failure")))
		r[handoffRerun(id*10)] = stepAnswer{"201", "{}"}
	}
	return r
}

func TestCodeReviewGateHandoffRerun(t *testing.T) {
	script := extractRunStep(t, "Re-run a newer twin's failed gate on this head")
	// HANDOFF_POLL_S=0: the step's poll of a run still in progress does not sleep.
	env := []string{"EVENT_SHA=" + stepHead, "BASE_REF=main", "HANDOFF_POLL_S=0"}
	cases := []stepCase{
		{
			name: "permitted: a newer completed run of this PR on this head whose gate failed or was cancelled",
			routes: with(handoffRoutes([]string{
				handoffRun(600, 1001, "completed", stepPR, "main"),
				handoffRun(601, 1002, "completed", stepPR, "main"),
			}, 600, 601), supersedeJobs(601), ok(jobsJSON(jobJSON(6010, "Code Review (codex)", "completed", "cancelled")))),
			posts: []string{handoffRerun(6000), handoffRerun(6010)},
			want:  []string{"Run 600 is newer on " + stepHead, "Run 601 is newer on " + stepHead},
		},
		{
			name: "other PR or base: a twin title for PR #43, PR #4 or another base is never re-run",
			routes: handoffRoutes([]string{
				handoffRun(610, 1001, "completed", "43", "main"),
				handoffRun(611, 1002, "completed", "4", "main"),
				handoffRun(612, 1003, "completed", stepPR, "release"),
			}, 610, 611, 612),
		},
		{
			name: "older or same run: a suite at or before this one is never re-run",
			routes: handoffRoutes([]string{
				handoffRun(500, handoffSuite, "completed", stepPR, "main"),
				handoffRun(400, 999, "completed", stepPR, "main"),
			}, 500, 400),
		},
		{
			name:   "no newer run: nothing is read past the list",
			routes: handoffRoutes(nil),
			want:   []string{"No newer run of PR 42 on " + stepHead},
		},
		{
			name: "gate not red: a green, skipped or running gate and a red review are not re-run",
			routes: with(with(with(handoffRoutes([]string{
				handoffRun(620, 1001, "completed", stepPR, "main"),
				handoffRun(621, 1002, "completed", stepPR, "main"),
				handoffRun(622, 1003, "completed", stepPR, "main"),
			}, 620, 621, 622),
				supersedeJobs(620), ok(jobsJSON(jobJSON(6200, "Code Review (codex)", "completed", "success")))),
				supersedeJobs(621), ok(jobsJSON(jobJSON(6210, "Code Review (codex)", "in_progress", "")))),
				supersedeJobs(622), ok(jobsJSON(jobJSON(6220, "Codex review", "completed", "failure"),
					jobJSON(6221, "Code Review (codex)", "completed", "skipped")))),
			want: []string{"Run 620: its gate did not end red", "Run 621: its gate did not end red", "Run 622: its gate did not end red"},
		},
		{
			// v5.12.11: a red gate in a run not yet completed is read again
			// (the run, then its jobs) until the run ends, then re-run.
			name: "run still active: a red gate in a run not completed is polled until the run ends, then re-run",
			routes: with(handoffRoutes([]string{
				handoffRun(630, 1001, "in_progress", stepPR, "main"),
			}, 630), "GET /actions/runs/630", ok(`{"id":630,"status":"completed"}`)),
			posts: []string{handoffRerun(6300)},
			want:  []string{"Run 630 is in_progress; reading it again in 0 s."},
		},
		{
			// Recorded behaviour: a refused re-run (the run was re-run or
			// changed between list and POST) warns and stops the step; this
			// run's pass stands and the newer gate stays red.
			name: "race: a re-run refused after the list is a warning that stops the step",
			routes: with(handoffRoutes([]string{
				handoffRun(640, 1001, "completed", stepPR, "main"),
				handoffRun(641, 1002, "completed", stepPR, "main"),
			}, 640, 641), handoffRerun(6400), stepAnswer{"403", `{"message":"This workflow is already running"}`}),
			posts: []string{handoffRerun(6400)},
			want:  []string{"POST /actions/jobs/6400/rerun answered HTTP 403; a newer twin's gate is not re-run"},
			warn:  true,
		},
		{
			name: "API error: an unreadable run list re-runs nothing",
			routes: with(handoffRoutes([]string{
				handoffRun(650, 1001, "completed", stepPR, "main"),
			}, 650), handoffList(), stepAnswer{"500", "{}"}),
			want: []string{"answered HTTP 500"},
			warn: true,
		},
		{
			name: "API error: an unreachable read of this run re-runs nothing",
			routes: with(handoffRoutes([]string{
				handoffRun(660, 1001, "completed", stepPR, "main"),
			}, 660), "GET /actions/runs/"+stepRunID, stepAnswer{"unreachable", ""}),
			want: []string{"cannot reach " + stepAPIBase + "/actions/runs/" + stepRunID},
			warn: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { checkStepCase(t, script, env, c) })
	}
}
