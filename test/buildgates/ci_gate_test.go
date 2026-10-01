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
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var ciWorkflowPath = filepath.Join(repoRoot, ".github", "workflows", "ci.yml")

// gateJobName is the job branch protection requires, as
// `CI / CI Gate (pull_request)`.
const gateJobName = "ci-gate"

// ciJobs returns every top-level job id in ci.yml, in file order.
func ciJobs(t *testing.T, body string) []string {
	t.Helper()
	var jobs []string
	inJobs := false
	for _, line := range strings.Split(body, "\n") {
		if line == "jobs:" {
			inJobs = true
			continue
		}
		if !inJobs {
			continue
		}
		if m := regexp.MustCompile(`^  ([a-z0-9][a-z0-9-]*):\s*$`).FindStringSubmatch(line); m != nil {
			jobs = append(jobs, m[1])
		}
	}
	if len(jobs) < 2 {
		t.Fatalf("found %d jobs in %s; discovery has broken and every assertion "+
			"below would pass over an empty set", len(jobs), ciWorkflowPath)
	}
	return jobs
}

// TestCIGateDependsOnEveryJobAndChecksEveryDependency is the gate's own gate.
//
// Branch protection can require exactly ONE status context, so `CI Gate` speaks
// for the whole pipeline. Two ways that claim rots, both silent:
//
//   - a NEW JOB is added and not wired into `needs`. The gate goes green while
//     that job is red, which is worse than having no gate: the rollup's whole
//     value is that a reader can trust one context.
//   - a job IS in `needs` but has no `check` line. `needs` only orders
//     execution; with `if: always()` the gate runs regardless of the
//     dependency's result, so an unchecked dependency is decorative.
//
// The second is the nastier one, because the job appears in the workflow and
// appears in the gate, and still gates nothing.
func TestCIGateDependsOnEveryJobAndChecksEveryDependency(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	body := string(raw)

	jobs := ciJobs(t, body)
	var gateFound bool
	var substantive []string
	for _, j := range jobs {
		if j == gateJobName {
			gateFound = true
			continue
		}
		substantive = append(substantive, j)
	}
	if !gateFound {
		t.Fatalf("ci.yml has no %q job. Branch protection requires the single "+
			"context it publishes (CI / CI Gate); without it the only options are "+
			"to require nothing or to require one leg and leave the rest advisory.",
			gateJobName)
	}

	// The gate's own `needs:` list.
	needsRe := regexp.MustCompile(`(?m)^  ` + gateJobName + `:\n(?:.*\n)*?    needs: \[([^\]]*)\]`)
	m := needsRe.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("%s has no `needs: [...]` line. A gate that depends on nothing "+
			"reports success the moment the workflow starts.", gateJobName)
	}
	var needs []string
	for _, n := range strings.Split(m[1], ",") {
		if n = strings.TrimSpace(n); n != "" {
			needs = append(needs, n)
		}
	}

	// Every `check <job> "..."` line in the gate's script.
	var checked []string
	for _, c := range regexp.MustCompile(`(?m)^\s+check\s+(\S+)\s+"`).FindAllStringSubmatch(body, -1) {
		checked = append(checked, c[1])
	}
	if len(checked) == 0 {
		t.Fatal("the CI Gate job runs no `check` lines at all; it passes unconditionally")
	}

	sort.Strings(substantive)
	sort.Strings(needs)
	sort.Strings(checked)

	if diff := missing(substantive, needs); len(diff) > 0 {
		t.Errorf("jobs exist in ci.yml but are NOT in %s's needs: %v\n\n"+
			"The gate would report success while these are red. Add each to `needs` "+
			"AND to a `check` line.", gateJobName, diff)
	}
	if diff := missing(needs, checked); len(diff) > 0 {
		t.Errorf("jobs are in %s's needs but have NO `check` line: %v\n\n"+
			"`needs` only orders execution. With `if: always()` the gate runs whatever "+
			"the dependency did, so an unchecked dependency gates nothing.", gateJobName, diff)
	}
	if diff := missing(checked, needs); len(diff) > 0 {
		t.Errorf("jobs have a `check` line but are NOT in %s's needs: %v\n\n"+
			"`needs.<job>.result` is empty for a job that is not a dependency, so the "+
			"check reads a blank string and never matches `success` — it would fail "+
			"the gate permanently.", gateJobName, diff)
	}
}

// TestCIGateRunsEvenWhenADependencyFails pins `if: always()`.
//
// Without it the gate is SKIPPED when any dependency fails. A skipped job never
// publishes its status, so a required `CI Gate` context would simply never
// arrive and the merge would block on silence rather than fail on a verdict —
// from the outside, identical to a pipeline that never ran.
func TestCIGateRunsEvenWhenADependencyFails(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	gate := regexp.MustCompile(`(?m)^  ` + gateJobName + `:\n(?:(?:    |      |\n).*\n)*`).FindString(string(raw))
	if gate == "" {
		t.Fatalf("could not isolate the %s job body", gateJobName)
	}
	if !regexp.MustCompile(`(?m)^    if: always\(\)\s*$`).MatchString(gate) {
		t.Errorf("%s is missing `if: always()`:\n%s\n\n"+
			"Without it the gate is skipped when a dependency fails, publishes no "+
			"status, and a required context never arrives.", gateJobName, gate)
	}
	// Fail-closed: anything that is not success must be treated as failure.
	if !strings.Contains(gate, `case "$2" in success)`) {
		t.Errorf("%s's check does not compare against `success` explicitly. It must "+
			"treat skipped and cancelled as failure — a job skipped because its own "+
			"dependency failed has not passed.", gateJobName)
	}
}

// missing returns the elements of want that are absent from got.
func missing(want, got []string) []string {
	have := make(map[string]bool, len(got))
	for _, g := range got {
		have[g] = true
	}
	var out []string
	for _, w := range want {
		if !have[w] {
			out = append(out, w)
		}
	}
	return out
}

// workflowTriggers returns the set of event names under a workflow's `on:`
// key, read from the PARSED document rather than from the file's text.
//
// This gate used to isolate `on:` with a regex and then run
// `strings.Contains(block, "pull_request")` over the raw slice, comments and
// all. On 2026-09-06 a comment was added inside claude-review.yml's `on:` block
// explaining WHY the pull_request trigger had been removed — and the word
// `pull_request` in that explanation reddened `main` for every PR in the repo,
// including the ones whose CI change was meant to be measured against it.
//
// A raw-text grep cannot tell a trigger from a sentence about a trigger, and
// the fix is not to reword the comment: that leaves the next engineer to
// launder wording past a check, and costs an explanation worth keeping. The
// parser has the answer exactly and comments are invisible to it.
//
// `on` survives as a `!!str` key here because gopkg.in/yaml.v3 does not apply
// the YAML 1.1 boolean resolution that turns `on:` into `true:` in some other
// parsers (PyYAML does). Verified against these workflows; if that ever
// changes, this returns an empty set and every caller t.Fatal's on it rather
// than silently passing.
func workflowTriggers(t *testing.T, path string, raw []byte) map[string]bool {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s is not parseable YAML: %v", path, err)
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		t.Fatalf("%s has no top-level mapping", path)
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "on" {
			continue
		}
		events := make(map[string]bool)
		switch v := root.Content[i+1]; v.Kind {
		case yaml.MappingNode: // on: {push: ..., pull_request: ...}
			for j := 0; j+1 < len(v.Content); j += 2 {
				events[v.Content[j].Value] = true
			}
		case yaml.SequenceNode: // on: [push, pull_request]
			for _, e := range v.Content {
				events[e.Value] = true
			}
		case yaml.ScalarNode: // on: push
			events[v.Value] = true
		}
		return events
	}
	return nil
}

// TestClaudeReviewIsBothUnpublishedAndUnrunnable pins BOTH halves of the
// disable, because each alone leaves a different hole open.
//
// The `pull_request` trigger is what registers a `Claude Review / Claude
// Review` commit status — one that stays PENDING FOREVER when the job is
// skipped, which becomes a permanent merge block once a status check is
// required. Removing the trigger fixes that.
//
// But `workflow_dispatch` is an EXECUTABLE manual trigger. Removing the
// trigger while also removing `if: false` made this workflow more reachable,
// not less: a dispatch would run the review worker on the credential-bearing
// `claude` runner, with the token supplied and PR_NUM empty, because a dispatch
// event carries no pull_request object.
//
// So: no PR trigger (publishes nothing) AND `if: false` (runs never).
func TestClaudeReviewIsBothUnpublishedAndUnrunnable(t *testing.T) {
	requirePrivateWorkflowFile(t, ".github/workflows/claude-review.yml")
	path := filepath.Join(repoRoot, ".github", "workflows", "claude-review.yml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	body := string(raw)

	triggers := workflowTriggers(t, path, raw)
	if len(triggers) == 0 {
		t.Fatalf("no `on:` block in %s", path)
	}
	if triggers["pull_request"] {
		t.Error("claude-review.yml triggers on pull_request. Gitea then registers a " +
			"`Claude Review / Claude Review` status for a job that never reports, so it " +
			"stays pending forever — a merge blocked on silence once a status check is " +
			"required.")
	}
	if !regexp.MustCompile(`(?m)^    if: false\s*$`).MatchString(body) {
		t.Error("claude-review.yml's job is missing `if: false`. `workflow_dispatch` is " +
			"an executable manual trigger, so without it the job can be dispatched and " +
			"will run the review worker on the credential-bearing `claude` runner with " +
			"an empty PR_NUM.")
	}
}
