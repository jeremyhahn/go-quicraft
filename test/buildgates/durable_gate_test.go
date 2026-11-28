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
	"strings"
	"testing"
)

// makefileAssign returns the right-hand side of the first `name :=` or
// `name ?=` assignment in makefile, failing if there is none.
func makefileAssign(t *testing.T, makefile, name string) string {
	t.Helper()
	for _, line := range strings.Split(makefile, "\n") {
		rest, ok := strings.CutPrefix(line, name)
		if !ok {
			continue
		}
		rest = strings.TrimLeft(rest, " \t")
		for _, op := range []string{":=", "?="} {
			if v, ok := strings.CutPrefix(rest, op); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	t.Fatalf("Makefile assigns no %s", name)
	return ""
}

func hasField(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// TestDurableSuiteInIntegrationGate fails if test/durable, the
// durable-by-default quorum-fsync test (#1309), drops out of what CI runs:
// IT_TEST_DIRS (the integration shards, via `make it-shard-packages`),
// IT_PKGS (`make integration-test`), or its own make target. Without it the
// quorum-fsync assertion would run by hand only.
func TestDurableSuiteInIntegrationGate(t *testing.T) {
	makefile := readRepoFile(t, makefilePath)

	if got := makefileAssign(t, makefile, "DURABLE_DIR"); got != "./test/durable" {
		t.Fatalf("DURABLE_DIR = %q, want ./test/durable", got)
	}
	if dirs := strings.Fields(makefileAssign(t, makefile, "IT_TEST_DIRS")); !hasField(dirs, "$(DURABLE_DIR)") {
		t.Errorf("IT_TEST_DIRS = %v: the CI integration shards skip test/durable", dirs)
	}
	if pkgs := strings.Fields(makefileAssign(t, makefile, "IT_PKGS")); !hasField(pkgs, "$(DURABLE_DIR)/...") {
		t.Errorf("IT_PKGS = %v: make integration-test skips test/durable", pkgs)
	}
	recipe := strings.Join(makeRecipe(t, makefile, "integration-test-durable-by-default"), "\n")
	if !strings.Contains(recipe, "./test/durable/...") && !strings.Contains(recipe, "$(DURABLE_DIR)/...") {
		t.Errorf("integration-test-durable-by-default does not run test/durable:\n%s", recipe)
	}
}
