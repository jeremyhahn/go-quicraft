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
	"strconv"
	"strings"
	"testing"
)

// fuzzWorkflowPath is the nightly, which owns the calibration.
var fuzzWorkflowPath = filepath.Join(workflowsDir, "fuzz.yml")

// pkgBudget matches a per-package budget line: EXECS= in fuzz.yml or base= in the Makefile.
var pkgBudget = regexp.MustCompile(`(\./pkg/[a-z/]+)\)\s+(?:EXECS|base)=(\d+)`)

// targetBudget matches a per-target override in either file:
//
//	FuzzMessageFrameDecode) echo 160000 ;;          (fuzz.yml)
//	FuzzMessageFrameDecode) base=160000 ;;          (Makefile)
var targetBudget = regexp.MustCompile(`((?:Fuzz\w+\|?\\?\s*)+)\)\s*(?:echo|base=)\s*(\d+)`)

// budgets returns package->execs and target->execs from one file.
func budgets(t *testing.T, path string) (pkgs, targets map[string]int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	body := string(raw)
	pkgs, targets = map[string]int{}, map[string]int{}
	for _, m := range pkgBudget.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[2])
		pkgs[m[1]] = n
	}
	for _, m := range targetBudget.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[2])
		for _, name := range strings.Split(strings.ReplaceAll(strings.ReplaceAll(m[1], "\\", ""), "\n", ""), "|") {
			if name = strings.TrimSpace(name); strings.HasPrefix(name, "Fuzz") {
				targets[name] = n
			}
		}
	}
	if len(pkgs) == 0 {
		t.Fatalf("found no per-package budgets in %s; discovery has broken and this "+
			"gate is comparing nothing", path)
	}
	return pkgs, targets
}

// TestFuzzBudgetsAgreeBetweenTheNightlyAndTheReleaseGate checks the Makefile budgets equal fuzz.yml's divided by FUZZ_MUT_DIVISOR, for the same packages.
func TestFuzzBudgetsAgreeBetweenTheNightlyAndTheReleaseGate(t *testing.T) {
	nightlyPkgs, nightlyTargets := budgets(t, fuzzWorkflowPath)
	releasePkgs, releaseTargets := budgets(t, makefilePath)

	// The divisor the Makefile documents.
	raw, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}
	m := regexp.MustCompile(`(?m)^FUZZ_MUT_DIVISOR\s*\?=\s*(\d+)`).FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatal("no FUZZ_MUT_DIVISOR in the Makefile; the release gate's relationship to " +
			"the nightly's calibration is then undocumented and unverifiable")
	}
	divisor, _ := strconv.Atoi(m[1])
	if divisor < 1 {
		t.Fatalf("FUZZ_MUT_DIVISOR is %d; it must be at least 1", divisor)
	}

	for _, tc := range []struct {
		what             string
		nightly, release map[string]int
	}{
		{"package", nightlyPkgs, releasePkgs},
		{"target override", nightlyTargets, releaseTargets},
	} {
		for k, want := range tc.nightly {
			got, ok := tc.release[k]
			if !ok {
				t.Errorf("%s %q has a budget in fuzz.yml but none in the Makefile.\n\n"+
					"The release gate would then refuse it outright, or fuzz it at another "+
					"package's budget. Add it to ci-fuzz-mutation.", tc.what, k)
				continue
			}
			if got != want {
				t.Errorf("%s %q: fuzz.yml has %d, Makefile has %d — expected the same number, "+
					"since both read the one calibration and the divisor is applied at run "+
					"time, not baked in.", tc.what, k, want, got)
			}
		}
		for k := range tc.release {
			if _, ok := tc.nightly[k]; !ok {
				t.Errorf("%s %q has a budget in the Makefile but none in fuzz.yml.\n\n"+
					"fuzz.yml owns the calibration; a number that exists only in the release "+
					"gate was never measured.", tc.what, k)
			}
		}
	}
}

// TestReleaseFuzzGateBudgetsByCountAndDoesNotRetry checks the release fuzz gate uses exec counts, not durations, and no retry.
func TestReleaseFuzzGateBudgetsByCountAndDoesNotRetry(t *testing.T) {
	raw, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}
	recipe := regexp.MustCompile(`(?m)^ci-fuzz-mutation:\n(?:\t.*\n)+`).FindString(string(raw))
	if recipe == "" {
		t.Fatal("could not isolate the ci-fuzz-mutation recipe")
	}

	if !strings.Contains(recipe, "-fuzztime=$${n}x") {
		t.Error("ci-fuzz-mutation does not budget by execution count. A duration budget " +
			"installs a deadline context, and the coordinator's shutdown race then reports " +
			"`context deadline exceeded` with no reproducer — a failure that is not a finding " +
			"`-fuzztime=<N>x` creates no deadline at all.")
	}
	if strings.Contains(recipe, "FUZZ_MUT_TIME") {
		t.Error("ci-fuzz-mutation still references FUZZ_MUT_TIME, a duration budget")
	}
	if strings.Contains(recipe, "context deadline exceeded") {
		t.Error("ci-fuzz-mutation still classifies `context deadline exceeded`. With the " +
			"deadline removed that outcome cannot occur, and keeping the branch means a " +
			"real failure matching the string would be retried instead of reported.")
	}
	if strings.Contains(recipe, "FUZZ_MUT_TRIES") || strings.Contains(recipe, "retry") {
		t.Error("ci-fuzz-mutation still retries. A retry on a gate is a filter on failure; " +
			"the flake it worked around is gone.")
	}
}
