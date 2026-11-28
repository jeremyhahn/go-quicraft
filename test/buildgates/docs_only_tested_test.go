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
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// docsOnlyStepName is the preflight step that classifies a PR as docs-only.
const docsOnlyStepName = "Is every changed file docs or markdown?"

// docsOnlyStep returns the workflow env and the run script of ci.yml's
// docs-only classifier step.
func docsOnlyStep(t *testing.T) (map[string]string, string) {
	t.Helper()
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	var wf struct {
		Env  map[string]string `yaml:"env"`
		Jobs map[string]struct {
			Steps []suiteStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", ciWorkflowPath, err)
	}
	for _, j := range wf.Jobs {
		for _, s := range j.Steps {
			if s.Name == docsOnlyStepName {
				return wf.Env, s.Run
			}
		}
	}
	t.Fatalf("%s: no step named %q", ciWorkflowPath, docsOnlyStepName)
	return nil, ""
}

// runDocsOnlyStep runs the classifier script on a pull request whose
// changed files are files, with a stub git that prints them, and returns
// the docs_only output.
func runDocsOnlyStep(t *testing.T, wfEnv map[string]string, script string, files []string) string {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not found")
	}
	dir := t.TempDir()
	list := filepath.Join(dir, "files")
	if err := os.WriteFile(list, []byte(strings.Join(files, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\nexec cat \"$CHANGED_FILES\"\n"
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	cmd := exec.CommandContext(t.Context(), bash, "-e", "-c", script)
	cmd.Env = []string{
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"CHANGED_FILES=" + list,
		"GITHUB_OUTPUT=" + out,
		"EVENT=pull_request",
		"BASE=base",
		"HEAD=head",
		"CI_FULL_LABEL=false",
	}
	for _, k := range []string{"DOCS_ONLY_PATTERN", "DOCS_TESTED_PATTERN", "LINT_GATES_PATTERN"} {
		if v, ok := wfEnv[k]; ok {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("classifier step failed: %v\n%s", err, b)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(got), "\n") {
		if v, ok := strings.CutPrefix(line, "docs_only="); ok {
			return v
		}
	}
	t.Fatalf("classifier wrote no docs_only output:\n%s", got)
	return ""
}

// TestDocsOnlyExcludesMarkdownTestsRead holds #1250: markdown a _test.go
// reads (pkg/metrics_documented_test.go, contrib/dragonboat/readme_table_test.go)
// is not documentation, so a PR changing only it runs the unit jobs.
func TestDocsOnlyExcludesMarkdownTestsRead(t *testing.T) {
	wfEnv, script := docsOnlyStep(t)
	for _, tc := range []struct {
		files []string
		want  string
	}{
		{[]string{"docs/guide.md"}, "true"},
		{[]string{"README.md", "docs/raft-consensus/engineering.md"}, "true"},
		{[]string{"docs/observability/metrics.md"}, "false"},
		{[]string{"docs/observability/engineering.md"}, "false"},
		{[]string{"docs/multi-group-host/api.md"}, "false"},
		{[]string{"contrib/dragonboat/README.md"}, "false"},
		{[]string{"docs/guide.md", "contrib/dragonboat/README.md"}, "false"},
		{[]string{"pkg/host.go"}, "false"},
	} {
		if got := runDocsOnlyStep(t, wfEnv, script, tc.files); got != tc.want {
			t.Errorf("changed files %v: docs_only=%s, want %s", tc.files, got, tc.want)
		}
	}
}
