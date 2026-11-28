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

package durable_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// makefileVar returns the right-hand side of the first `name :=` or
// `name ?=` assignment in the Makefile.
func makefileVar(t *testing.T, mk, name string) string {
	t.Helper()
	for _, line := range strings.Split(mk, "\n") {
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

// TestDurableSuiteInIntegrationGate fails if this package drops out of the
// integration packages CI runs: IT_TEST_DIRS (the CI shards, via
// `make it-shard-packages`) and IT_PKGS (`make integration-test`). Without
// it the quorum-fsync assertion of durable_test.go would be run by hand only.
func TestDurableSuiteInIntegrationGate(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	mk := string(raw)

	if got := makefileVar(t, mk, "DURABLE_DIR"); got != "./test/durable" {
		t.Fatalf("DURABLE_DIR = %q, want ./test/durable", got)
	}
	dirs := strings.Fields(makefileVar(t, mk, "IT_TEST_DIRS"))
	if !contains(dirs, "$(DURABLE_DIR)") {
		t.Errorf("IT_TEST_DIRS = %v: the CI integration shards skip test/durable", dirs)
	}
	pkgs := strings.Fields(makefileVar(t, mk, "IT_PKGS"))
	if !contains(pkgs, "$(DURABLE_DIR)/...") {
		t.Errorf("IT_PKGS = %v: make integration-test skips test/durable", pkgs)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
