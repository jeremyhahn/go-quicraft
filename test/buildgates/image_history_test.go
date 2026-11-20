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
	"regexp"
	"strings"
	"testing"
)

var (
	envRefRe    = regexp.MustCompile(`\benv\.([A-Z_][A-Z0-9_]*)`)
	shellRefRe  = regexp.MustCompile(`\$\{?([A-Z_][A-Z0-9_]*)`)
	envAssignRe = regexp.MustCompile(`\b([A-Z_][A-Z0-9_]*)=`)
	envKeyRe    = regexp.MustCompile(`(?m)^ +([A-Z_][A-Z0-9_]*):`)
)

// imageHistoryProblems returns, for each step of job that runs
// scripts/ci-image-history.sh, the variables its `if:` or `run:` reads that
// no earlier step of the job writes to GITHUB_ENV and no env: block sets.
// A guard on such a variable is always false, so the check never runs.
func imageHistoryProblems(job, workflowEnv string) []string {
	declared := map[string]bool{}
	for _, m := range envKeyRe.FindAllStringSubmatch(workflowEnv, -1) {
		declared[m[1]] = true
	}
	var problems []string
	steps := strings.Split(job, "\n      - ")
	for _, step := range steps {
		if strings.Contains(step, "scripts/ci-image-history.sh") {
			var refs []string
			for _, line := range strings.Split(step, "\n") {
				t := strings.TrimSpace(line)
				switch {
				case strings.HasPrefix(t, "if:"):
					for _, m := range envRefRe.FindAllStringSubmatch(t, -1) {
						refs = append(refs, m[1])
					}
				case strings.HasPrefix(t, "run:") && strings.Contains(t, "ci-image-history.sh"):
					for _, m := range shellRefRe.FindAllStringSubmatch(t, -1) {
						refs = append(refs, m[1])
					}
				}
			}
			if len(refs) == 0 {
				problems = append(problems, "an image-history step names no image variable")
			}
			for _, r := range refs {
				if !declared[r] {
					problems = append(problems, r+" is read by an image-history step but no earlier step sets it")
				}
			}
		}
		for _, line := range strings.Split(step, "\n") {
			if strings.Contains(line, "GITHUB_ENV") {
				for _, m := range envAssignRe.FindAllStringSubmatch(line, -1) {
					declared[m[1]] = true
				}
			}
		}
		for _, m := range envKeyRe.FindAllStringSubmatch(step, -1) {
			declared[m[1]] = true
		}
	}
	return problems
}

// TestImageHistoryStepsReadSetVariables pins every image-history check to
// variables the job sets before it. A check keyed on a variable nothing sets
// is skipped on every run, and the images go unchecked without a red step.
func TestImageHistoryStepsReadSetVariables(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	body := string(raw)
	head, _, ok := strings.Cut(body, "\njobs:\n")
	if !ok {
		t.Fatalf("no jobs: block in %s", ciWorkflowPath)
	}
	checked := 0
	for _, j := range ciJobs(t, body) {
		job := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(j) + `:\n(?:(?:    |      |\n).*\n)*`).FindString(body)
		checked += strings.Count(job, "scripts/ci-image-history.sh")
		for _, p := range imageHistoryProblems(job, head) {
			t.Errorf("job %q: %s", j, p)
		}
	}
	if checked == 0 {
		t.Fatal("no job runs scripts/ci-image-history.sh; step discovery has broken")
	}
}

// TestImageHistoryProblemsCatchesAnUnsetVariable is the check's own check:
// a guard on a variable no step sets must be reported.
func TestImageHistoryProblemsCatchesAnUnsetVariable(t *testing.T) {
	good := "  it:\n    steps:\n" +
		"      - name: Name the integration image\n" +
		"        run: echo \"IT_IMAGE=$(make -s it-image)\" >> \"$GITHUB_ENV\"\n" +
		"      - name: Check\n" +
		"        if: ${{ !cancelled() && env.IT_IMAGE != '' && runner.environment != 'github-hosted' }}\n" +
		"        run: scripts/ci-image-history.sh \"$IT_IMAGE\"\n"
	if p := imageHistoryProblems(good, ""); len(p) != 0 {
		t.Errorf("a set variable was reported: %v", p)
	}
	bad := strings.ReplaceAll(good, "env.IT_IMAGE", "env.IT_LEASE_ID")
	bad = strings.ReplaceAll(bad, `"$IT_IMAGE"`, `"$IT_LEASE_ID"`)
	if p := imageHistoryProblems(bad, ""); len(p) == 0 {
		t.Error("a guard on IT_LEASE_ID, which no step sets, was not reported")
	}
	late := "  it:\n    steps:\n" +
		"      - name: Check\n" +
		"        if: ${{ env.IT_IMAGE != '' }}\n" +
		"        run: scripts/ci-image-history.sh \"$IT_IMAGE\"\n" +
		"      - name: Name the integration image\n" +
		"        run: echo \"IT_IMAGE=x\" >> \"$GITHUB_ENV\"\n"
	if p := imageHistoryProblems(late, ""); len(p) == 0 {
		t.Error("a variable set only after the check was not reported")
	}
}
