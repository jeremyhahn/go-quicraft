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
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

// suiteStep is the part of a workflow step the integration-suite check reads.
type suiteStep struct {
	Name string `yaml:"name"`
	ID   string `yaml:"id"`
	If   string `yaml:"if"`
	Run  string `yaml:"run"`
}

// integrationSuiteProblems reports how an integration job's steps could run
// the suite on an image the image-history check has not passed: no step
// before the check builds a missing image, the check is skipped on some
// condition other than a hosted runner (or has no id to key on), or the
// suite step does not require the check's success off hosted runners.
func integrationSuiteProblems(steps []suiteStep) []string {
	var probs []string
	check := -1
	for i, s := range steps {
		if strings.Contains(s.Run, "scripts/ci-image-history.sh") && strings.Contains(s.Run, "$IT_IMAGE") {
			check = i
			break
		}
	}
	if check < 0 {
		return []string{"no step runs scripts/ci-image-history.sh on $IT_IMAGE"}
	}
	built := false
	for _, s := range steps[:check] {
		if strings.Contains(s.Run, "docker image inspect") && strings.Contains(s.Run, "make docker-build-it") {
			built = true
		}
	}
	if !built {
		probs = append(probs, "no step before the image-history check builds the "+
			"integration image when the daemon lacks it (make docker-build-it)")
	}
	c := steps[check]
	if c.ID == "" {
		probs = append(probs, "the image-history check has no id for the suite to key on")
	}
	if strings.Contains(c.If, "IT_LEASE") {
		probs = append(probs, "the image-history check is skipped when IT_LEASE is "+
			"empty, so a missing image runs unchecked")
	}
	suite := -1
	for i, s := range steps {
		if strings.Contains(s.Run, "make integration-test") {
			suite = i
		}
	}
	switch {
	case suite < 0:
		probs = append(probs, "no step runs make integration-test")
	case suite < check:
		probs = append(probs, "make integration-test runs before the image-history check")
	default:
		cond := steps[suite].If
		if c.ID == "" || !strings.Contains(cond, "steps."+c.ID+".outcome == 'success'") ||
			!strings.Contains(cond, "runner.environment == 'github-hosted'") {
			probs = append(probs, fmt.Sprintf("make integration-test's if: %q does not "+
				"require the image-history check's success off hosted runners", cond))
		}
	}
	return probs
}

func TestIntegrationSuiteRunsOnlyOnACheckedImage(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	var wf struct {
		Jobs map[string]struct {
			Steps []suiteStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", ciWorkflowPath, err)
	}
	job, ok := wf.Jobs["integration"]
	if !ok {
		t.Fatalf("%s: no integration job", ciWorkflowPath)
	}
	for _, p := range integrationSuiteProblems(job.Steps) {
		t.Errorf("integration job: %s", p)
	}

	// The form this replaces: the lease step leases only an image already on
	// the daemon, the check is keyed on the lease, and the suite builds a
	// missing image and runs it unchecked.
	old := []suiteStep{
		{Name: "Lease the integration image", Run: "if docker image inspect \"$IT_IMAGE\"; then docker create; fi"},
		{Name: "Check", If: "${{ !cancelled() && env.IT_IMAGE != '' && env.IT_LEASE != '' && runner.environment != 'github-hosted' }}",
			Run: "scripts/ci-image-history.sh \"$IT_IMAGE\""},
		{Name: "make integration-test", Run: "make integration-test IT_IMAGE=\"$IT_IMAGE\""},
	}
	if p := integrationSuiteProblems(old); len(p) < 3 {
		t.Errorf("the lease-keyed form was not fully reported: %v", p)
	}
}
