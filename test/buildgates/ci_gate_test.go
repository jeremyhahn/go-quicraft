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
	// ci-template v1.21 breaks the line after `in`.
	if !regexp.MustCompile(`case "\$2" in\s+success\)`).MatchString(gate) {
		t.Errorf("%s's check does not compare against `success` explicitly. It must "+
			"treat skipped and cancelled as failure — a job skipped because its own "+
			"dependency failed has not passed.", gateJobName)
	}
}

// fullTierIf is the condition every full-tier job carries, and the
// expression ci-gate evaluates into FULL_TIER (#293).
const fullTierIf = `github.event_name == 'push' || github.event_name == 'workflow_dispatch' || contains(github.event.pull_request.labels.*.name, 'ci:full')`

// TestCIGateTiersMatchTheJobs pins the two tiers to the gate.
//
// ci-gate passes a skipped job only when its check line tags it `full` and
// the run did not select the full tier. Two drifts would break that: a job
// tagged `full` whose `if:` differs from FULL_TIER, so the gate excuses a skip
// in a run that should have executed it; and a PR-tier job that gains an
// `if:`, so it skips on some event and fails the gate there.
func TestCIGateTiersMatchTheJobs(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	body := string(raw)

	if !strings.Contains(body, "FULL_TIER: ${{ "+fullTierIf+" }}") {
		t.Errorf("%s's FULL_TIER is not the full-tier condition %q", gateJobName, fullTierIf)
	}

	tiers := map[string]string{}
	for _, c := range regexp.MustCompile(`(?m)^\s+check\s+(\S+)\s+"[^"]*"\s*(\S*)\s*$`).FindAllStringSubmatch(body, -1) {
		tiers[c[1]] = c[2]
	}

	for _, j := range ciJobs(t, body) {
		if j == gateJobName {
			continue
		}
		job := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(j) + `:\n(?:(?:    |      |\n).*\n)*`).FindString(body)
		hasFullIf := strings.Contains(job, "\n    if: "+fullTierIf+"\n")
		switch tiers[j] {
		case "full":
			if !hasFullIf {
				t.Errorf("job %q is tagged full in %s but lacks `if: %s`", j, gateJobName, fullTierIf)
			}
		case "pr":
			if regexp.MustCompile(`(?m)^    if:`).MatchString(job) {
				t.Errorf("job %q is PR tier but carries a job-level `if:`; a PR-tier job "+
					"must run on every event, and %s fails it when skipped", j, gateJobName)
			}
		default:
			t.Errorf("job %q's check line in %s has tier %q; want `pr` or `full`",
				j, gateJobName, tiers[j])
		}
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
