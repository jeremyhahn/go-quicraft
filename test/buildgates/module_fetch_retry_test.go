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
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// toolchainDockerfile builds the CI toolchain image whose tools stage installs
// the lint and scan tools from the module proxy.
const toolchainDockerfile = "build/docker/devcontainer/Dockerfile"

// dragonboatMakefile holds the contrib module's CI targets.
const dragonboatMakefile = "contrib/dragonboat/Makefile"

// goFetchCommand matches a `go install` or `go get` invocation.
var goFetchCommand = regexp.MustCompile(`\bgo (install|get)\b`)

// dockerfileStage returns the lines of the named stage: from its
// `FROM ... AS <name>` line up to the next FROM.
func dockerfileStage(t *testing.T, body, name string) []string {
	t.Helper()
	var out []string
	in := false
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(trimmed), "FROM ") {
			if in {
				break
			}
			fields := strings.Fields(trimmed)
			in = len(fields) >= 4 && strings.EqualFold(fields[len(fields)-2], "AS") && fields[len(fields)-1] == name
			continue
		}
		if in {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s has no %q stage", toolchainDockerfile, name)
	}
	return out
}

// TestToolsStageRetriesEveryModuleFetch keeps one transient proxy error from
// failing the toolchain image build: every `go install` and `go get` in the
// tools stage goes through the stage's retry function.
func TestToolsStageRetriesEveryModuleFetch(t *testing.T) {
	stage := dockerfileStage(t, readFile(t, filepath.Join(repoRoot, toolchainDockerfile)), "tools")
	defined := false
	fetches := 0
	for _, line := range stage {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(trimmed, "retry()") {
			defined = true
		}
		for _, loc := range goFetchCommand.FindAllStringIndex(trimmed, -1) {
			fetches++
			if !strings.HasSuffix(trimmed[:loc[0]], "retry ") {
				t.Errorf("%s tools stage fetches modules without retry: %s", toolchainDockerfile, trimmed)
			}
		}
	}
	if !defined {
		t.Errorf("%s tools stage does not define a retry() function", toolchainDockerfile)
	}
	if fetches == 0 {
		t.Fatalf("%s tools stage has no go install/go get; the check proves nothing", toolchainDockerfile)
	}
}

// makeTargetPrereqs returns the prerequisites of a make rule `name: ...`.
func makeTargetPrereqs(body, name string) ([]string, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+":") && !strings.HasPrefix(line, name+":=") {
			return strings.Fields(strings.TrimPrefix(line, name+":")), true
		}
	}
	return nil, false
}

// TestDragonboatCITargetsDownloadModulesFirst keeps the contrib CI targets
// from fetching modules mid-test with no retry: they depend on mod-download,
// which retries `go mod download`.
func TestDragonboatCITargetsDownloadModulesFirst(t *testing.T) {
	body := readFile(t, filepath.Join(repoRoot, dragonboatMakefile))
	if _, ok := makeTargetPrereqs(body, "mod-download"); !ok {
		t.Fatalf("%s has no mod-download target", dragonboatMakefile)
	}
	if !strings.Contains(body, "mod download") {
		t.Errorf("%s mod-download does not run go mod download", dragonboatMakefile)
	}
	for _, target := range []string{"ci-test", "ci-integration"} {
		prereqs, ok := makeTargetPrereqs(body, target)
		if !ok {
			t.Errorf("%s has no %s target", dragonboatMakefile, target)
			continue
		}
		if !slices.Contains(prereqs, "mod-download") {
			t.Errorf("%s %s does not depend on mod-download (prerequisites %v)", dragonboatMakefile, target, prereqs)
		}
	}
}
