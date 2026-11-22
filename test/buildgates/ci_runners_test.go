// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buildgates

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The hosted-first runner rule (ci-template v1.46, code-review-template
// v5.12.4): every workflow job runs on hostedRunner, the standard
// GitHub-hosted runner. A job leaves it only with a waiver on its runs-on
// line that names the category of its reason:
//
//	runs-on: <label>  # self-hosted: hardware: <reason>
//	runs-on: <label>  # self-hosted: oversize: <reason>
//
// hardware is a device, GPU or dedicated host the job needs; oversize is
// more than a standard runner's 4 vCPU, 16 GB, about 45 GB of disk or 6
// hours. `credential:` is accepted only for code-review.yml's Codex review
// job, whose reviewer token is mounted on one runner. ci.yml and release.yml
// carry no waiver at all: every job in release.yml runs on hostedRunner, a
// job that names the `release` label included, and so does every ci.yml job
// but preflight, whose one pinned pool expression, preflightRunsOn
// (ci-template v1.49), falls back to hostedRunner off github.com and when
// the repository variable CI_PREFLIGHT_RUNS_ON says so, and (a local
// security divergence) for a pull request from a fork.
const (
	hostedRunner     = "ubuntu-latest"
	releaseLabel     = "release"
	selfHostedWaiver = "# self-hosted: "
	preflightRunsOn  = `${{ github.server_url == 'https://github.com' && (!startsWith(github.event_name, 'pull_request') || github.event.pull_request.head.repo.full_name == github.repository) && fromJSON(vars.CI_PREFLIGHT_RUNS_ON || '["self-hosted","linux","x64","bigdisk"]') || 'ubuntu-latest' }}`
)

// waiverCategories are the categories any waived job may name.
var waiverCategories = []string{"hardware", "oversize"}

// credentialJob is the one workflow job a `credential:` waiver may carry.
var credentialJob = struct{ file, job string }{"code-review.yml", "review"}

// hostedOnlyWorkflows are the workflows in which no job is waived.
var hostedOnlyWorkflows = []string{"ci.yml", "release.yml"}

// codeReviewTemplateRunsOn lists the runs-on expressions the code-review
// template carries as written; any other expression is a problem.
var codeReviewTemplateRunsOn = []string{
	`${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || 'bigdisk' }}`,
	`${{ github.server_url == 'https://github.com' && (github.event.pull_request.head.repo.full_name == github.repository && fromJSON('["self-hosted","linux","x64","bigdisk"]') || 'ubuntu-latest') || 'bigdisk' }}`,
}

// workflowJob is one job of a workflow and its runs-on node.
type workflowJob struct {
	name   string
	runsOn *yaml.Node
	uses   bool // a reusable-workflow call, which has no runs-on
}

// runnerJobs parses a workflow and returns its jobs in order, or a problem.
func runnerJobs(file string, body []byte) ([]workflowJob, string) {
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil || len(doc.Content) == 0 {
		return nil, file + " does not parse: " + errString(err)
	}
	jobs := mapValue(doc.Content[0], "jobs")
	if jobs == nil || len(jobs.Content) == 0 {
		return nil, file + " has no jobs"
	}
	var out []workflowJob
	for i := 0; i+1 < len(jobs.Content); i += 2 {
		def := jobs.Content[i+1]
		out = append(out, workflowJob{
			name:   jobs.Content[i].Value,
			runsOn: mapValue(def, "runs-on"),
			uses:   mapValue(def, "uses") != nil,
		})
	}
	return out, ""
}

// hasWaiver reports whether a runs-on line comment carries a waiver marker.
func hasWaiver(comment string) bool {
	return strings.Contains(comment+" ", strings.TrimSpace(selfHostedWaiver)+" ")
}

// waiverProblem checks the waiver in a runs-on line comment and returns ""
// when it names a category allowed for this job and a non-empty reason.
func waiverProblem(comment string, credentialOK bool) string {
	marker := strings.TrimSpace(selfHostedWaiver)
	i := strings.Index(comment+" ", marker+" ")
	if i < 0 {
		return "carries no `" + selfHostedWaiver + "hardware: <reason>` or `" + selfHostedWaiver + "oversize: <reason>` waiver"
	}
	rest := strings.TrimSpace(comment[i+len(marker):])
	category, reason, ok := strings.Cut(rest, ":")
	category = strings.TrimSpace(category)
	switch {
	case ok && category == "credential" && !credentialOK:
		return "has a credential: waiver, which only code-review.yml's Codex review job may carry"
	case !ok || (!slices.Contains(waiverCategories, category) && category != "credential"):
		return "has a waiver that names no category: want `" + selfHostedWaiver + "hardware: <reason>` or `" + selfHostedWaiver + "oversize: <reason>`, got " + strconv.Quote(comment)
	case strings.TrimSpace(reason) == "":
		return "has a " + category + ": waiver with an empty reason"
	}
	return ""
}

// runnerProblems lists every job of the workflow file (its base name) whose
// runs-on breaks the hosted-first rule, given the workflow's text.
func runnerProblems(file string, body []byte) []string {
	jobs, problem := runnerJobs(file, body)
	if problem != "" {
		return []string{problem}
	}
	hostedOnly := slices.Contains(hostedOnlyWorkflows, file)
	var problems []string
	for _, j := range jobs {
		where := file + " job " + j.name
		credentialOK := file == credentialJob.file && j.name == credentialJob.job
		switch {
		case j.runsOn == nil:
			if !j.uses {
				problems = append(problems, where+" has no runs-on")
			}
			continue
		case hostedOnly && hasWaiver(j.runsOn.LineComment):
			problems = append(problems, where+" carries a self-hosted waiver; every "+file+" job runs on "+hostedRunner)
			continue
		case j.runsOn.Kind != yaml.ScalarNode:
			problems = append(problems, where+" runs on a label list; a job runs on "+hostedRunner+" or one self-hosted label with a waiver")
			if p := waiverProblem(j.runsOn.LineComment, credentialOK); p != "" {
				problems = append(problems, where+" "+p)
			}
		case file == "ci.yml" && j.name == "preflight" && j.runsOn.Value == preflightRunsOn:
			continue
		case file == "code-review.yml" && slices.Contains(codeReviewTemplateRunsOn, j.runsOn.Value) && !hasWaiver(j.runsOn.LineComment):
			// The template's server-conditional expressions: the hosted image on
			// github.com (the pool only for a same-repository head under a pull
			// request event, which the Runner labels step checks), bigdisk off it.
			continue
		case strings.Contains(j.runsOn.Value, "${{"):
			problems = append(problems, where+" runs on an expression, which can pick a self-hosted label: "+strconv.Quote(j.runsOn.Value))
		case j.runsOn.Value == hostedRunner:
			if hasWaiver(j.runsOn.LineComment) {
				problems = append(problems, where+" runs on "+hostedRunner+" but carries a self-hosted waiver")
			}
		case strings.EqualFold(strings.TrimSpace(j.runsOn.Value), releaseLabel):
			// Runner labels match case-insensitively; a waiver never sends a job to the release runner.
			problems = append(problems, where+" runs on the release label ("+strconv.Quote(j.runsOn.Value)+"); no waiver puts a job on it")
		case hostedOnly:
			problems = append(problems, where+" runs on "+strconv.Quote(j.runsOn.Value)+"; every "+file+" job runs on "+hostedRunner)
		default:
			if p := waiverProblem(j.runsOn.LineComment, credentialOK); p != "" {
				problems = append(problems, where+" runs on "+strconv.Quote(j.runsOn.Value)+" and "+p)
			}
		}
	}
	return problems
}

// mapValue returns the value of key in the mapping n, or nil.
func mapValue(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return "empty document"
	}
	return err.Error()
}

func readWorkflow(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(workflowsDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return b
}

// TestCIRunsHostedFirst holds the hosted-first rule on every workflow file.
func TestCIRunsHostedFirst(t *testing.T) {
	entries, err := os.ReadDir(workflowsDir)
	if err != nil {
		t.Fatalf("read %s: %v", workflowsDir, err)
	}
	seen := 0
	for _, e := range entries {
		if e.IsDir() || !isWorkflowFile(e.Name()) {
			continue
		}
		seen++
		for _, p := range runnerProblems(e.Name(), readWorkflow(t, e.Name())) {
			t.Error(p)
		}
	}
	if seen < 3 {
		t.Fatalf("found %d workflow files in %s; discovery has broken", seen, workflowsDir)
	}
}

// TestCIPreflightRunsOnPinned keeps ci.yml's preflight runs-on equal to
// preflightRunsOn, so the one expression runnerProblems admits cannot drift
// from the workflow without this test failing.
func TestCIPreflightRunsOnPinned(t *testing.T) {
	jobs, problem := runnerJobs("ci.yml", readWorkflow(t, "ci.yml"))
	if problem != "" {
		t.Fatal(problem)
	}
	for _, j := range jobs {
		if j.name != "preflight" {
			continue
		}
		if j.runsOn == nil || j.runsOn.Value != preflightRunsOn {
			t.Fatalf("ci.yml job preflight runs-on is not the pinned pool expression %q", preflightRunsOn)
		}
		return
	}
	t.Fatal("ci.yml has no preflight job")
}

// templateConcurrencyGroup is ci-template v1.53's concurrency group: on main
// the group is `main-{event}`, so a newer main run cancels the older one; a
// schedule run is `schedule-{repository}`; every other ref is its own group.
const templateConcurrencyGroup = "${{ github.workflow }}-${{ github.event_name == 'schedule' && " +
	"format('schedule-{0}', github.repository) || github.ref == 'refs/heads/main' && " +
	"format('main-{0}', github.event_name) || github.ref }}"

// forkPullRequestRunner resolves a job's runs-on for a pull request whose head
// repository is a fork. A literal label resolves, and so does preflight's
// pinned preflightRunsOn, whose fork clause gives a fork's pull request
// ubuntu-latest whatever CI_PREFLIGHT_RUNS_ON holds. Any other expression can
// read variables and job outputs this test cannot know, so it reports false.
func forkPullRequestRunner(runsOn *yaml.Node) (string, bool) {
	if runsOn != nil && runsOn.Kind == yaml.ScalarNode && runsOn.Value == preflightRunsOn {
		return hostedRunner, true
	}
	if runsOn == nil || runsOn.Kind != yaml.ScalarNode || strings.Contains(runsOn.Value, "${{") {
		return "", false
	}
	return runsOn.Value, true
}

// forkRunnerProblems lists each ci.yml job that a fork's pull request could
// run off the hosted runner (ci-template v1.52).
func forkRunnerProblems(body []byte) []string {
	jobs, problem := runnerJobs("ci.yml", body)
	if problem != "" {
		return []string{problem}
	}
	var problems []string
	for _, j := range jobs {
		if j.uses {
			continue
		}
		if got, ok := forkPullRequestRunner(j.runsOn); !ok || got != hostedRunner {
			problems = append(problems, "ci.yml job "+j.name+" does not resolve to "+hostedRunner+" for a fork's pull request")
		}
	}
	return problems
}

// TestCIForkPullRequestRunsHosted pins ci-template v1.52's outcome: a fork's
// pull request runs every ci.yml job on ubuntu-latest.
func TestCIForkPullRequestRunsHosted(t *testing.T) {
	body := readWorkflow(t, "ci.yml")
	for _, p := range forkRunnerProblems(body) {
		t.Error(p)
	}
	expr := `${{ fromJSON(vars.CI_PREFLIGHT_RUNS_ON || '["self-hosted","linux","x64","bigdisk"]') }}`
	for _, c := range []struct{ name, job, runsOn string }{
		{"a self-hosted label", "coverage", "bigdisk"},
		{"an expression that can pick a self-hosted label", "preflight", expr},
	} {
		got := forkRunnerProblems([]byte(replaceJobRunner(t, string(body), c.job, c.runsOn)))
		if len(got) != 1 || !strings.Contains(got[0], "job "+c.job+" ") {
			t.Errorf("%s: want one finding for job %s, got %q", c.name, c.job, got)
		}
	}
}

// TestCIConcurrencyGroupIsMainPerEvent pins ci-template v1.53: main's
// concurrency group is `main-{event}` and cancel-in-progress is a literal true.
func TestCIConcurrencyGroupIsMainPerEvent(t *testing.T) {
	var doc yaml.Node
	if err := yaml.Unmarshal(readWorkflow(t, "ci.yml"), &doc); err != nil || len(doc.Content) == 0 {
		t.Fatalf("ci.yml does not parse: %s", errString(err))
	}
	c := mapValue(doc.Content[0], "concurrency")
	if c == nil {
		t.Fatal("ci.yml has no concurrency block")
	}
	if g := mapValue(c, "group"); g == nil || g.Value != templateConcurrencyGroup {
		t.Errorf("ci.yml concurrency.group is not ci-template v1.53's `main-{event}` group:\n got %v\nwant %q", g, templateConcurrencyGroup)
	}
	if v := mapValue(c, "cancel-in-progress"); v == nil || v.Value != "true" || v.Tag != "!!bool" {
		t.Errorf("ci.yml cancel-in-progress is not a literal true")
	}
}

// TestRunnerProblems shows that runnerProblems reports each regression of
// the runner rule, and accepts a categorised waiver where one is allowed.
func TestRunnerProblems(t *testing.T) {
	const expr = `${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || 'bigdisk' }}`
	cr := func(job, runsOn string) func(string) (string, string) {
		return func(b string) (string, string) { return "code-review.yml", replaceJobRunner(t, b, job, runsOn) }
	}
	ci := func(job, runsOn string) func(string) (string, string) {
		return func(b string) (string, string) { return "ci.yml", replaceJobRunner(t, b, job, runsOn) }
	}
	for _, c := range []struct {
		name   string
		mutate func(string) (string, string)
		want   string // "" when the mutation is allowed
	}{
		{"a job sent to a self-hosted label", cr("scope", "bigdisk"), "job scope runs on \"bigdisk\" and carries no"},
		{"a job sent to a label list", cr("scope", "[self-hosted, linux, x64, bigdisk]"), "job scope runs on a label list"},
		{"an expression the template does not carry", cr("gate", `${{ github.server_url == 'https://github.com' && 'bigdisk' || 'ubuntu-latest' }}`), "job gate runs on an expression"},
		{"the template's expression is allowed", cr("scope", expr), ""},
		{"an expression with a waiver", cr("gate", expr+"  "+selfHostedWaiver+"oversize: big"), "job gate runs on an expression"},
		{"a bare waiver", cr("scope", "bigdisk  # self-hosted:"), "names no category"},
		{"a waiver with no category", cr("scope", "bigdisk  "+selfHostedWaiver+"needs a big disk"), "names no category"},
		{"an unknown category", cr("scope", "bigdisk  "+selfHostedWaiver+"speed: faster"), "names no category"},
		{"a credential waiver off the Codex job", cr("review-opencode", "qwen  "+selfHostedWaiver+"credential: a token"), "credential: waiver, which only"},
		{"a hardware waiver with an empty reason", cr("scope", "bigdisk  "+selfHostedWaiver+"hardware: "), "hardware: waiver with an empty reason"},
		{"an oversize waiver with an empty reason", cr("scope", "bigdisk  "+selfHostedWaiver+"oversize:"), "oversize: waiver with an empty reason"},
		{"a credential waiver with an empty reason", cr("review", "codex  "+selfHostedWaiver+"credential:"), "credential: waiver with an empty reason"},
		{"a waiver on a hosted line", cr("scope", hostedRunner+"  "+selfHostedWaiver+"hardware: none"), "runs on ubuntu-latest but carries a self-hosted waiver"},
		{"a ci.yml job sent to a self-hosted label", ci("coverage", "bigdisk"), "ci.yml job coverage runs on \"bigdisk\"; every ci.yml job"},
		{"a waived upper-case release label", cr("scope", "RELEASE  "+selfHostedWaiver+"hardware: dedicated host"), "job scope runs on the release label"},
		{"a waived mixed-case release label", cr("scope", "Release  "+selfHostedWaiver+"hardware: dedicated host"), "job scope runs on the release label"},
		{"a waived ci.yml job", ci("coverage", "bigdisk  "+selfHostedWaiver+"oversize: measured 40 GB of disk"), "ci.yml job coverage carries a self-hosted waiver"},
		{"a ci.yml expression", ci("coverage", expr), "ci.yml job coverage runs on an expression"},
		{"preflight's pool expression on another job", ci("coverage", preflightRunsOn), "ci.yml job coverage runs on an expression"},
		{"preflight's pool expression changed", ci("preflight", strings.Replace(preflightRunsOn, `"bigdisk"`, `"bigdisk-docker"`, 1)), "ci.yml job preflight runs on an expression"},
		{"preflight back on the hosted runner is allowed", ci("preflight", hostedRunner), ""},
		{"a hardware waiver is allowed", cr("scope", "bigdisk  "+selfHostedWaiver+"hardware: needs the attached device"), ""},
		{"an oversize waiver is allowed", cr("scope", "bigdisk  "+selfHostedWaiver+"oversize: measured 40 GB of disk"), ""},
		{"a waiver after another comment is allowed", cr("review-opencode", "qwen  # CUSTOMIZE: the label "+selfHostedWaiver+"hardware: the model host"), ""},
		{"the Codex job's credential waiver is allowed", cr("review", "codex  "+selfHostedWaiver+"credential: the token is here"), ""},
		{"the Gitea release arm put back", ci("publish-images", `${{ github.server_url == 'https://github.com' && 'ubuntu-latest' || 'release' }}`), "ci.yml job publish-images runs on an expression"},
	} {
		t.Run(c.name, func(t *testing.T) {
			file, _ := c.mutate("")
			body := string(readWorkflow(t, file))
			if p := runnerProblems(file, []byte(body)); len(p) != 0 {
				t.Fatalf("%s itself has problems: %v", file, p)
			}
			_, mutated := c.mutate(body)
			if mutated == body {
				t.Fatal("the mutation changed nothing")
			}
			p := runnerProblems(file, []byte(mutated))
			if c.want == "" {
				if len(p) != 0 {
					t.Fatalf("want no problem, got %v", p)
				}
				return
			}
			if !slices.ContainsFunc(p, func(s string) bool { return strings.Contains(s, c.want) }) {
				t.Fatalf("want a problem containing %q, got %v", c.want, p)
			}
		})
	}
}

// TestRunnerProblems_Unparsable shows a broken or empty workflow is reported.
func TestRunnerProblems_Unparsable(t *testing.T) {
	for in, want := range map[string]string{
		"jobs: [":                      "does not parse",
		"on: push\n":                   "has no jobs",
		"jobs:\n  a:\n    steps: []\n": "job a has no runs-on",
	} {
		if p := runnerProblems("x.yml", []byte(in)); len(p) != 1 || !strings.Contains(p[0], want) {
			t.Errorf("runnerProblems(%q) = %v; want one problem containing %q", in, p, want)
		}
	}
	if p := runnerProblems("x.yml", []byte("jobs:\n  a:\n    uses: ./.github/workflows/b.yml\n")); len(p) != 0 {
		t.Errorf("a reusable-workflow call has no runs-on and needs none; got %v", p)
	}
}

// replaceJobRunner replaces the runs-on line of job in the workflow text;
// with an empty body it returns it unchanged, so a case can name its file.
func replaceJobRunner(t *testing.T, body, job, runsOn string) string {
	t.Helper()
	if body == "" {
		return ""
	}
	start := strings.Index(body, "\n  "+job+":\n")
	if start < 0 {
		t.Fatalf("the workflow has no job %s", job)
	}
	i := strings.Index(body[start:], "    runs-on: ")
	if i < 0 {
		t.Fatalf("job %s has no runs-on line", job)
	}
	i += start
	end := strings.Index(body[i:], "\n") + i
	return body[:i] + "    runs-on: " + runsOn + body[end:]
}
