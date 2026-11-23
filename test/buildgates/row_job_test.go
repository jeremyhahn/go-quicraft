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
	"testing"
)

// rowJobTests is the Row job's own test, from the CI template plus the LOCAL
// case R13: it reads the "Row check" step's python out of ci.yml and runs it
// against a temporary git repository and a local labels API.
const rowJobTests = "testdata/row-job-tests.py"

func TestRowJobCases(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git not found; %s needs it", rowJobTests)
	}
	runTemplateTests(t, rowJobTests)
}
