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

import "testing"

// runnerSelectTests is the preflight step `Balanced runner pool (ruling
// 1137)`'s own test, from the CI template: it reads the step's run block out
// of ci.yml and runs it against a fake API, checking the runner and
// runner_docker outputs it writes (the hosted default, ubuntu-latest, on any
// doubt; no call off github.com; a fork's head stays hosted).
const runnerSelectTests = "testdata/runner-select-tests.py"

func TestRunnerSelectStepPicksPool(t *testing.T) {
	runTemplateTests(t, runnerSelectTests)
}
