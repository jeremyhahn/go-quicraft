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
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// runnerLabelsTests is the Runner labels step's own test: it reads the step's
// python out of ci.yml and runs its permitted and forbidden cases.
const runnerLabelsTests = "test/buildgates/testdata/runner-labels-tests.py"

func TestRunnerLabelsStepRejects(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found; the lint-gates job runs these cases")
	}
	out, err := exec.CommandContext(t.Context(), py, "-I", "testdata/runner-labels-tests.py").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "ALL PASS") {
		t.Fatalf("%s: err %v, want exit 0 and ALL PASS; output:\n%s", runnerLabelsTests, err, out)
	}
}

func TestRunnerLabelsTestsAreWiredIntoCI(t *testing.T) {
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
	job, ok := wf.Jobs["lint-gates"]
	if !ok {
		t.Fatalf("%s: no lint-gates job", ciWorkflowPath)
	}
	want := "python3 -I " + runnerLabelsTests
	for _, s := range job.Steps {
		if strings.TrimSpace(s.Run) == want {
			return
		}
	}
	t.Errorf("%s: the lint-gates job has no step running %q", ciWorkflowPath, want)
}
