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
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// runnerLabelsTests is the preflight step `Runner labels (ruling 633)`'s own
// test, from the CI template: it reads the step's python out of ci.yml, runs
// its permitted and forbidden cases, and reads every job's runs-on in ci.yml
// with the step's own reader.
const runnerLabelsTests = "testdata/runner-labels-tests.py"

// runTemplateTests runs one of the CI template's python test scripts and
// fails unless it exits 0 and reports ALL PASS.
func runTemplateTests(t *testing.T, script string) {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		// The preflight job runs both scripts directly on every CI run
		// (TestPreflightRunsTheRunnerTemplateTests), so a toolchain image
		// without python3 skips here without leaving them unchecked.
		t.Skipf("python3 not found; %s needs it", script)
	}
	out, err := exec.CommandContext(t.Context(), py, "-I", script).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "ALL PASS") {
		t.Fatalf("%s: err %v, want exit 0 and ALL PASS; output:\n%s", script, err, out)
	}
}

func TestRunnerLabelsStepRejects(t *testing.T) {
	runTemplateTests(t, runnerLabelsTests)
}

// TestPreflightRunsTheRunnerTemplateTests pins the mandatory preflight step
// that runs both template test scripts with python3 on every CI run: the Go
// wrappers above skip where python3 is absent, so this step is what makes the
// scripts a required check.
func TestPreflightRunsTheRunnerTemplateTests(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	start := strings.Index(body, "\n  preflight:\n")
	if start < 0 {
		t.Fatal("ci.yml has no preflight job")
	}
	job := body[start+len("\n  preflight:\n"):]
	if loc := regexp.MustCompile(`\n  [^ \n]`).FindStringIndex(job); loc != nil {
		job = job[:loc[0]]
	}
	const step = "      - name: Runner template tests\n"
	i := strings.Index(job, step)
	if i < 0 {
		t.Fatalf("preflight has no %q step", strings.TrimSpace(step))
	}
	block := job[i+len(step):]
	if j := strings.Index(block, "\n      - "); j >= 0 {
		block = block[:j]
	}
	if strings.Contains(block, "if:") || strings.Contains(block, "continue-on-error") {
		t.Errorf("the Runner template tests step must run unconditionally; got:\n%s", block)
	}
	for _, script := range []string{runnerLabelsTests, runnerSelectTests, rowJobTests} {
		want := "python3 -I test/buildgates/" + script
		if !strings.Contains(block, want) {
			t.Errorf("the Runner template tests step does not run %q", want)
		}
	}
}
