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
	"os/exec"
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
